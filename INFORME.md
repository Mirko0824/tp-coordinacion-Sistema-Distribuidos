Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

## Coordinación y Sincronización de las instancias de Sum

En la etapa de suma parcial de las frutas, cuando el cliente termina de enviar todos los mensajes, envía un último mensaje (`EOF_CLIENT`) para finalizar la transmisión. Este mensaje es asignado a una sola instancia de Sum de forma aleatoria por el middleware. Esto provoca que:

**El nodo que recibe el mensaje (`EOF_CLIENT`) es el único que sabe que el cliente terminó de enviar mensajes. Las demás instancias no tienen forma de saber que ya pueden enviar sus sumas parciales.**

Para solucionar este problema, cuando una instancia de Sum recibe el `EOF_CLIENT`, extrae la cantidad total de mensajes enviados por el cliente (`message_total_amount`) que se calcula en el (`message_handler`) del Gateway. Luego realiza un broadcast de un mensaje de tipo `EOF_CONTROL` a traves del exchange (`SUM_CONTROL_EXCHANGE`) de tipo `fanout`, que replica el mensaje a todas las instancias, incluyendo a si mismo. De esta manera se notifica a todos los Sum que el cliente termino de enviar mensajes y que ya pueden proceder con la suma parcial.

En cada Sum se divide el trabajo en dos threads de ejecución concurrentes:

- **Thread principal:** Se encarga de consumir los registros de frutas de la cola de entrada (`INPUT_QUEUE`) y sumar las cantidades en un diccionario en memoria (`clients_fruit_sum`), clasificado por cliente y por fruta.
- **Thread secundario (`sum_eof_control`):** Dedicado exclusivamente a escuchar los mensajes de coordinación del exchange de control, enviar la suma parcial a los aggregations y eliminar al cliente y sus frutas de la memoria compartida.
Dado que ambos threads acceden a la misma estructura de datos (`clients_fruit_sum`), se utiliza un lock de exclusión mutua (`clients_sum_lock`) para proteger el recurso compartido.

Sin embargo, aunque el Lock previene race conditions de lectura y escritura, surge el siguiente problema de coordinación:

**La instancia de Sum que recibe el (EOF_CONTROL), sabe que el cliente termino de enviar mensajes, pero no sabe si todavía existen mensajes pendientes de procesamiento. Por lo tanto, si envía su suma parcial inmediatamente, esta podría estar incompleta.**

Para solucionar este segundo problema, se implementó un mecanismo de barrera de sincronización que asegura que ninguna instancia de Sum envíe sus sumas parciales hasta que todos los mensajes del cliente hayan sido procesados.

- **Identificador único por mensaje (`message_id`):** En el Gateway, el (`message_handler`) contabiliza el total de mensajes enviados por el cliente y se asigna un (`message_id`) incremental a cada mensaje de fruta.
- **Notificación de progreso (`SUM_PROCESSED`):** Cada vez que el thread principal de cualquier Sum procesa y acumula una fruta en la memoria compartida, envía un mensaje de tipo (`SUM_PROCESSED`) con el (`client_id`) y el (`message_id`) a través del exchange de control (`SUM_CONTROL_EXCHANGE`). Usando el exchange_type (`fanout`), esta notificación le llega a todos los Sums, incluyéndose a sí mismo.
- **Registro de mensajes procesados y barrera:** Cada Sum mantiene en memoria un conjunto (`set`) con los ids de los mensajes procesados (`clients_processed_messages`). Cuando el thread principal procesa un (`FRUIT_INFO`), guarda el (`message_id`) en ese set. A su vez, mediante los mensajes (`SUM_PROCESSED`) recibe y registra los ids procesados por las demás instancias. Al utilizar un set, si un mismo identificador es recibido más de una vez no se contabiliza de forma duplicada.
- **Comprobación de la condición de fin:** Cuando el Sum recibe (`SUM_PROCESSED`) o al recibir (`EOF_CONTROL`) se valida si la cantidad de mensajes procesados es igual al total de mensajes del cliente. En caso de ser iguales se confirma que no queda ningún mensaje pendiente en ningún Sum. Recién en ese momento, el thread secundario envía la suma parcial a los aggregations y elimina el cliente y sus frutas del diccionario compartido.

## Coordinación entre las instancias de Sum y Aggregation

Para que cada Aggregator reciba una fruta en especifico y la suma parcial correspondiente, cada Sum recorre las frutas acumuladas del cliente y hashea el nombre de la fruta (`hashlib.sha256`). A partir de este valor se aplica el módulo sobre la cantidad de Aggregators. De esta manera, se garantiza que **todas las sumas parciales de una misma fruta siempre lleguen a la misma instancia de Aggregator**.

Una vez que un Sum envía todas sus sumas parciales de un cliente, envía un mensaje (`EOF_SUM`) a todos los Aggregators. Cada Aggregator mantiene, para cada cliente, el registro de las instancias de Sum de las que recibió (`EOF_SUM`). Cuando recibió el (`EOF_SUM`) de las (`SUM_AMOUNT`) instancias, sabe que no va a recibir mas sumas parciales para ese cliente, por lo que calcula el top parcial de frutas y se lo envía a join.

## Escalabilidad

Cada cliente se identifica mediante un (`client_id`) generado por UUID4. Esto permite procesar múltiples clientes en simultaneo.

A pesar de que la implementación es escalable y segura frente a race conditions, usando la barrera implementada. Entiendo que no es una solución del todo eficiente y optima tener que guardar grandes cantidades de datos hasta que todos hayan terminado de procesar todos los mensajes. Para un gran volúmen de datos esto podría causar problemas de rendimiento y consumo de memoria.

Por otro lado, por cada mensaje procesada se hace un broadcast notificando a todos. Ante volúmenes de datos masivos, este flujo constante de notificaciones puede saturar el broker y convertirse en el principal cuello de botella del sistema.
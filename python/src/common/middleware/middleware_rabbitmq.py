import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.consumer_connection = None
        self.producer_connection = None
        self.consumer_channel = None
        self.producer_channel = None
        self.is_consuming = False

    def send(self, message):
        try:
            # Valido que la conexion y el canal no existan o ya esten cerrados para volver a crearlos
            if self.producer_connection is None or self.producer_connection.is_closed:
                # Se inicializa la conexion con rabbitmq de parte del productor
                self.producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            if self.producer_channel is None or self.producer_channel.is_closed:
                # Se crea un canal dentro de la conexion establecida para no tener que establecer multiples conexiones 
                self.producer_channel = self.producer_connection.channel()
        except Exception as error:
            # Levanto excepcion de conexion
            raise MessageMiddlewareDisconnectedError(error)

        try:
            # Creo una cola con el nombre que me pasan y el parametro durable true para que persista
            self.producer_channel.queue_declare(queue=self.queue_name, durable=True)
            # Se envia/publica el mensaje en rabbitmq, donde el mensaje se va a encolar segun lo definido en routing_key
            self.producer_channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        
        except Exception as error:
            # Levanto excepcion por algun error interno
            raise MessageMiddlewareMessageError(error)
            
    
    def start_consuming(self, on_message_callback):
        try:
            # Se inicializa la conexion con rabbitmq de parte del consumidor y se crea el canal dentro de la conexion
            # Valido que la conexion y el canal no existan o ya esten cerrados para volver a crearlos
            if self.consumer_connection is None or self.consumer_connection.is_closed:
                self.consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            if self.consumer_channel is None or self.consumer_channel.is_closed:
                self.consumer_channel = self.consumer_connection.channel()
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)

        if self.is_consuming:
            raise MessageMiddlewareMessageError("Ya se esta consumiendo la cola, no se puede volver a consumir")
        
        try:
            # Creo la cola donde se encolan los mensajes
            self.consumer_channel.queue_declare(queue=self.queue_name, durable=True)

            # Defino la funcion de callback para que cuando se reciba un mensaje de la cola se ejecute esta funcion
            # Es una funcion inermediaria para recibir los 4 parametros y transformar esa informacion
            def callback(channel, method, properties, body):
                # Defino las funciones closure ack y nack
                # Llamo al metodo basic_ack que manda un ack a rabbitmq confirmando que salio bien y que puede desencolar el mensaje
                # Le paso por parametro el codigo del mensaje (delvery_tag)
                ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
                # Llamo al metodo basic_nack para avisar que hubo un error, defino requeue false para queno se encole nuevamente el mensaje
                # Le paso por parametro el codigo del mensaje (delvery_tag)
                nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                # Llamo a on_message_callback y paso por parametro las variables
                on_message_callback(body, ack, nack)

            # Defino que quiero consumir de la cola declarada con el nombre self.queue_name y paso la funcion callback
            self.consumer_channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.is_consuming = True
            # Empieza a recibir los mensajes de la cola
            self.consumer_channel.start_consuming()
        except Exception as error:
            raise MessageMiddlewareMessageError(error)
        finally:
            self.is_consuming = False

    def close(self):
        try:
            # Verifico que exista la conexcion de productor/consumidor y verifico que la conexion esta activa para cerrarlo
            if self.producer_connection and self.producer_connection.is_open:
                self.producer_connection.close()
            if self.consumer_connection and self.consumer_connection.is_open:
                self.consumer_connection.close()
        except Exception as error:
            # Levanto excepcion de close por error interno
            raise MessageMiddlewareCloseError(error)
    
    def stop_consuming(self):
        try:
            if self.is_consuming and self.consumer_connection and self.consumer_connection.is_open:
                self.consumer_connection.add_callback_threadsafe(
                    self.consumer_channel.stop_consuming
                )
        except Exception as error:
            # Levanto excepcion de conexion
            raise MessageMiddlewareDisconnectedError(error)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, exchange_type='direct'):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_connection = None
        self.producer_connection = None
        self.consumer_channel = None
        self.producer_channel = None
        self.is_consuming = False
        self.exchange_type = exchange_type

    def start_consuming(self, on_message_callback):
        try:
            # Inicializo conexion con rabbitmq y creo canal
            if self.consumer_connection is None or self.consumer_connection.is_closed:
                self.consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            if self.consumer_channel is None or self.consumer_channel.is_closed:
                self.consumer_channel = self.consumer_connection.channel()
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)
        
        if self.is_consuming:
            raise MessageMiddlewareMessageError("Ya se esta consumiendo el exchange, no se puede volver a consumir")

        try:
            self.consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

            # Creo una queue name concatenando todos los routing keys con el exchange name
            queue_name = f"{self.exchange_name}_{'_'.join(self.routing_keys)}"
            self.consumer_channel.queue_declare(queue=queue_name, durable=True)
            
            # Si no es de tipo fanout para broadcast, necesito recorrer las routing keys y bindear para escuchar mensajes con ese key
            if self.exchange_type != 'fanout':
                for rk in self.routing_keys:
                    self.consumer_channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=rk)
            else:
                self.consumer_channel.queue_bind(exchange=self.exchange_name, queue=queue_name)
            
            def callback(channel, method, properties, body):
                ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                on_message_callback(body, ack, nack)

            self.consumer_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            self.is_consuming = True
            self.consumer_channel.start_consuming()
        except Exception as error:
            raise MessageMiddlewareMessageError(error)
        finally:
            self.is_consuming = False

    def send(self, message):
        try:
            if self.producer_connection is None or self.producer_connection.is_closed:
                self.producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            if self.producer_channel is None or self.producer_channel.is_closed:
                self.producer_channel = self.producer_connection.channel()
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)
        
        try:
            self.producer_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

            # Si no es de tipo fanout para broadcast, necesito recorrer las routing keys y publicar el mensaje con cada una
            if self.exchange_type != 'fanout':
                for rk in self.routing_keys:
                    self.producer_channel.basic_publish(exchange=self.exchange_name, routing_key=rk, body=message)
            else:
                self.producer_channel.basic_publish(exchange=self.exchange_name, routing_key='', body=message)    

        except Exception as error:
            raise MessageMiddlewareMessageError(error)

    def close(self):
        try:
            if self.producer_connection and self.producer_connection.is_open:
                self.producer_connection.close()
            if self.consumer_connection and self.consumer_connection.is_open:
                self.consumer_connection.close()
        except Exception as Error:
            raise MessageMiddlewareCloseError(Error)
    
    def stop_consuming(self):
        try:
            if self.is_consuming and self.consumer_connection and self.consumer_connection.is_open:
                self.consumer_connection.add_callback_threadsafe(
                    self.consumer_channel.stop_consuming
                )
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)
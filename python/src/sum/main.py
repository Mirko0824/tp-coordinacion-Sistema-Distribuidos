import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        # Incializo json para clasificar cada fruta y cantidad respecto a su cliente correspondiente
        self.clients_fruit_sum = {}
        # Cada Sum escucha su routing key para recibir su copia del EOF_CONTROL
        self.eof_control_listener = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        # Creo un productor y paso el array de routing keys
        self.eof_control_exchange = (
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
            )
        )
        
        # Se crea un lock para proteger clients_fruit_sum, ya que 
        # el thread principal agrega y suma frutas y el thread secundario se encarga de leer y eliminar
        self.clients_sum_lock = threading.Lock()
        self.sum_eof_control = None

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.clients_fruit_sum[client_id] = self.clients_fruit_sum.setdefault(client_id, {})

        # Si la fruta ya existe, sumo la cantidad, sino se agrega
        self.clients_fruit_sum[client_id][fruit] = self.clients_fruit_sum[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
    
    def _notify_eof_control(self, client_id):
        eof_message = {
                "type": message_protocol.internal.MessageType.EOF_CONTROL,
                "client_id": client_id,
            }
        # Envio una copia del EOF_CONTROL a cada instancia de sum
        self.eof_control_exchange.send(message_protocol.internal.serialize(eof_message))

    def _send_parcial_sum(self, client_id, client_fruits):
        # Recorro cada fruta del json para enviarlos cada uno a las instancias de aggregation
        # Envio todas las frutas y sumas parciales acumuladas del client_id a todas las instancias de aggregation
        for parcial_fruit in client_fruits.values():
            parcial_fruit_message = {
                "type": message_protocol.internal.MessageType.PARCIAL_SUM,
                "client_id": client_id,
                "sum_id": ID,
                "fruit": parcial_fruit.fruit,
                "amount": parcial_fruit.amount,
            }

            # Calculo un hash a partir del nombre de la fruta
            fruit_hash = hashlib.sha256(parcial_fruit.fruit.encode("utf-8")).hexdigest()

            # Convierto el hash hexadecimal a un entero y calculo el modulo para obtener el aggregation_id
            aggregation_id = int(fruit_hash, 16) % AGGREGATION_AMOUNT

            # Envío la suma parcial únicamente al aggregation asignado
            self.data_output_exchanges[aggregation_id].send(message_protocol.internal.serialize(parcial_fruit_message))
    
    def _send_eof_sum(self, client_id):
        # Envio EOF_SUM a todos los aggregations para indicar que esta instancia terminó de enviar las sumas del cliente
        eof_message = {
            "type": message_protocol.internal.MessageType.EOF_SUM,
            "sum_id": ID,
            "client_id": client_id,
        }
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize(eof_message))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        # Obtengo las frutas y cantidades del client_id y si no encuentra devuelve json vacio
        client_fruits = self.clients_fruit_sum.get(client_id, {})
        self._send_parcial_sum(client_id, client_fruits)
        self._send_eof_sum(client_id)

        # Elimino todo el json del client_id
        self.clients_fruit_sum.pop(client_id, None)

    def process_eof_control(self, message, ack, nack):
        logging.info("Received EOF sum control")
        fields = message_protocol.internal.deserialize(message)
        if fields.get("type") != message_protocol.internal.MessageType.EOF_CONTROL:
            nack()
            return
        # Lockeo por si el thread principal está agregando frutas
        with self.clients_sum_lock:
            self._process_eof(fields["client_id"])
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        message_type = fields["type"]
        # Valido si el mensaje es de tipo data o eof
        if message_type == message_protocol.internal.MessageType.FRUIT_INFO:
            with self.clients_sum_lock:
                self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        # Cuando es EOF_CLIENT, la instancia de sum se encarga de notificar a sus hermanos y a si misma
        elif message_type == message_protocol.internal.MessageType.EOF_CLIENT:
            self._notify_eof_control(fields["client_id"])
        
        ack()

    def start(self):
        # Inicio un thread dedicado a recibir los EOF_CONTROL
        self.sum_eof_control = threading.Thread(
            target=self.eof_control_listener.start_consuming, 
            args=(self.process_eof_control,), 
            daemon=False
        )
        self.sum_eof_control.start()

        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.stop_consume()
            # Espero a que el thread termine
            if self.sum_eof_control is not None:
                self.sum_eof_control.join()

    def stop_consume(self):
        # Detengo la queue que recibe FRUIT_INFO y EOF_CLIENT
        self.input_queue.stop_consuming()
        # Detengo el exchange que recibe EOF_CONTROL
        self.eof_control_listener.stop_consuming()

    def handle_sigterm(self, signum, frame):
        # Al recibir SIGTERM llamo a stop
        self.stop_consume()

    def close_connections(self):
        # Cierro las conexiones de las colas y exchanges
        self.input_queue.close()
        self.eof_control_listener.close()
        self.eof_control_exchange.close()

        # Cierro los exchanges que envian los datos parciales a los aggregators
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    # Llamo el handler para el SIGTERM
    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close_connections()

    return 0


if __name__ == "__main__":
    main()

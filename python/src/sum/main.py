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
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        # Incializo json para clasificar cada fruta y cantidad respecto a su cliente correspondiente
        self.clients_fruit_sum = {}
        # Cantidad de mensajes procesados por cliente
        self.clients_processed_messages = {}
        # Total de mensajes por cliente
        self.clients_total_messages = {}
        # Guardo los message_ids ya procesados de cada cliente
        self.processed_message_ids = {}
        # Cada Sum escucha su routing key para recibir su copia del EOF_CONTROL
        self.eof_control_listener = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, 
            SUM_CONTROL_EXCHANGE, 
            [f"{SUM_PREFIX}_{ID}"], 
            'fanout'
        )
        # Creo un productor que hace broadcast, no necesita routing keys
        self.eof_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, 
            SUM_CONTROL_EXCHANGE, 
            [], 
            'fanout'
        )
        
        # Se crea un lock para proteger clients_fruit_sum, ya que 
        # el thread principal agrega y suma frutas y el thread secundario se encarga de leer y eliminar
        self.clients_sum_lock = threading.Lock()
        # Guardo la instancia del thread
        self.sum_eof_control = None

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        # Obtengo el cliente, si no existe creo uno nuevo
        client_fruit_sum = self.clients_fruit_sum.setdefault(client_id, {})

        # Si la fruta ya existe, sumo la cantidad, sino se agrega
        client_fruit_sum[fruit] = client_fruit_sum.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
    
    def _notify_eof_control(self, client_id, message_total_amount):
        eof_message = {
                "type": message_protocol.internal.MessageType.EOF_CONTROL,
                "client_id": client_id,
                "message_total_amount": message_total_amount,
            }
        # Envio una copia del EOF_CONTROL a cada instancia de sum
        self.eof_control_exchange.send(message_protocol.internal.serialize(eof_message))

    def _notify_sum_processed(self, client_id, message_id):
        processed_message = {
            "type": message_protocol.internal.MessageType.SUM_PROCESSED,
            "client_id": client_id,
            "message_id": message_id,
        }
        # Se lo envio a todos los sums para sincronizar el progreso
        self.eof_control_exchange.send(
            message_protocol.internal.serialize(processed_message)
        )

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

            # Envio la suma parcial unicamente al aggregation asignado
            self.data_output_exchanges[aggregation_id].send(message_protocol.internal.serialize(parcial_fruit_message))
    
    def _send_eof_sum(self, client_id):
        eof_message = {
            "type": message_protocol.internal.MessageType.EOF_SUM,
            "sum_id": ID,
            "client_id": client_id,
        }
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize(eof_message))

    def handle_eof_control(self):
        try:
            self.eof_control_listener.start_consuming(self.process_eof_control)
        finally:
            self.eof_control_listener.close()
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.close()

    def _process_eof(self, client_id, client_fruits):
        # Envio la suma parcial de cada fruta al aggregation correspondiente
        self._send_parcial_sum(client_id, client_fruits)
        # Envio EOF_SUM a todos los aggregations para indicar que esta instancia termino de enviar las sumas del cliente
        self._send_eof_sum(client_id)

    def _handle_finished_client(self, client_id):
        # Comparo la cantidad de mensajes procesados y el total de mensajes
        # para saber si es el ultimo mensaje del cliente
        expected_messages_amount = self.clients_total_messages.get(client_id)
        if expected_messages_amount is None:
            return
        
        processed_messages_amount = self.clients_processed_messages.get(client_id, set())
        if len(processed_messages_amount) != expected_messages_amount:
            return

        client_fruits = self.clients_fruit_sum.pop(client_id, {})
        self.clients_processed_messages.pop(client_id, None)
        self.clients_total_messages.pop(client_id, None)
        self.processed_message_ids.pop(client_id, None)

        return client_fruits

    def process_eof_control(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        message_type = fields.get("type")
        if message_type not in (
            message_protocol.internal.MessageType.EOF_CONTROL,
            message_protocol.internal.MessageType.SUM_PROCESSED,
        ):
            nack()
            return

        client_id = fields["client_id"]
        # El lock bloquea la suma local y el estado de la barrera
        # La barrera se comprueba tanto al recibir progreso como al recibir EOF
        # porque ambos tipos de mensaje pueden llegar en cualquier orden
        with self.clients_sum_lock:
            if message_type == message_protocol.internal.MessageType.SUM_PROCESSED:
                processed_messages = self.clients_processed_messages.setdefault(
                    client_id, set()
                )
                processed_messages.add(fields["message_id"])
            elif message_type == message_protocol.internal.MessageType.EOF_CONTROL:
                self.clients_total_messages[client_id] = fields["message_total_amount"]

            client_fruits = self._handle_finished_client(client_id)
        
        if client_fruits is not None:
            self._process_eof(client_id, client_fruits)

        ack()

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        message_type = fields["type"]

        if message_type not in (
            message_protocol.internal.MessageType.FRUIT_INFO,
            message_protocol.internal.MessageType.EOF_CLIENT,
        ):
            nack()
            return

        if message_type == message_protocol.internal.MessageType.FRUIT_INFO:
            message_id = fields["message_id"]
            client_id = fields["client_id"]
            with self.clients_sum_lock:
                # Si el mensaje ya fue procesado, lo descarto
                received_message_ids = self.processed_message_ids.setdefault(client_id, set())
                if message_id in received_message_ids:
                    ack()
                    return
                # Lockeo el acceso a la memoria compartida para guardar nueva fruta o cantidad
                self._process_data(client_id, fields["fruit"], fields["amount"])
                received_message_ids.add(message_id)
            # Aviso de forma broadcast despues de haber procesado el mensaje
            self._notify_sum_processed(client_id, message_id)
        # Cuando es EOF_CLIENT, la instancia de sum se encarga de notificar a sus hermanos y a si misma
        elif message_type == message_protocol.internal.MessageType.EOF_CLIENT:
            self._notify_eof_control(fields["client_id"], fields["message_total_amount"])
        
        ack()

    def start(self):
        # Inicio un thread dedicado a recibir los EOF_CONTROL
        self.sum_eof_control = threading.Thread(target=self.handle_eof_control, daemon=False)
        self.sum_eof_control.start()
        
        try:
            self.input_queue.start_consuming(self.process_data_message)
        finally:
            self.stop_consume()
            if self.sum_eof_control and self.sum_eof_control.is_alive():
                self.sum_eof_control.join()

    def stop_consume(self):
        # Detengo la queue que recibe FRUIT_INFO y EOF_CLIENT
        self.input_queue.stop_consuming()
        # Detengo el exchange que recibe EOF_CONTROL
        self.eof_control_listener.stop_consuming()

    def handle_sigterm(self, signum, frame):
        self.stop_consume()

    def close_connections(self):
        # Cierro las conexiones de las colas y exchanges
        self.input_queue.close()
        self.eof_control_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close_connections()

    return 0


if __name__ == "__main__":
    main()

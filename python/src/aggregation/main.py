import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients_fruits = {}
        self.clients_eof_sums = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_fruit = self.clients_fruits.setdefault(client_id, {})
        # Obtengo el fruitItem, si no existe devuelve un fruitItem con cantidad 0 y le suma el fruitItem con la cantidad recibida
        # Si ya existe la fruta, devuelve la cantidad que ya tenia y le suma el fruitItem con la cantidad recibida
        client_fruit[fruit] = client_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))
        # Guardo el top de frutas actualizado del client_id
        self.clients_fruits[client_id] = client_fruit
    
    def _sum_eof(self, client_id, sum_id):
        # Obtengo el set de sum_ids que ya enviaron eof para el client_id
        # si no existe devuelve un set vacio
        client_eof_sums = self.clients_eof_sums.setdefault(client_id, set())
        # Agrego el sum_id al set del cliente
        client_eof_sums.add(sum_id)
        # Devuelvo True si ya se recibieron todos los eof de los sum
        return len(client_eof_sums) == SUM_AMOUNT
    
    def _final_top(self, client_id):
        client_top = self.clients_fruits.get(client_id, {})
        # Obtengo el top de frutas ordenado por cantidad de mayor a menor
        fruit_top_items = sorted(client_top.values())
        fruit_top_items.reverse()
        # Obtengo el top de frutas limitado a TOP_SIZE
        fruit_top_items = fruit_top_items[:TOP_SIZE]
        # Guardo la fruta y la cantidad en una lista de tuplas para enviarlo a join
        top_fruits = []
        for fruit in fruit_top_items:
            top_fruits.append((fruit.fruit, fruit.amount))
        
        return top_fruits

    def _send_top_fruits(self, client_id, top_fruits):
        # Defino el json que envia a join con el top de frutas del client_id
        top_fruits_message = {
            "type": message_protocol.internal.MessageType.PARCIAL_TOP,
            "client_id": client_id,
            "aggregation_id": ID,
            "top_fruits": top_fruits,
        }
        self.output_queue.send(message_protocol.internal.serialize(top_fruits_message))

    def _process_eof(self, client_id, sum_id):
        logging.info("Received EOF")
        
        # Mientras no se recibieron todos los eof de los sum, no se envian los top parciales
        if not self._sum_eof(client_id, sum_id):
            return
        # Elimino el client_id una vez que se recibieron todos los eof
        self.clients_eof_sums.pop(client_id, None)
        # Obtengo el top de frutas
        top_fruits = self._final_top(client_id)
        self._send_top_fruits(client_id, top_fruits)
        # Elimino el client_id una vez que se enviaron los top parciales
        self.clients_fruits.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        message_type = fields.get("type")
        # Valido si el mensaje es de tipo data o eof
        if message_type == message_protocol.internal.MessageType.PARCIAL_SUM:
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        elif message_type == message_protocol.internal.MessageType.EOF_SUM:
            self._process_eof(fields["client_id"], fields["sum_id"])
        
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.stop_consume()

    def stop_consume(self):
        self.input_exchange.stop_consuming()

    def handle_sigterm(self, signum, frame):
        self.stop_consume()

    def close_connections(self):
        self.input_exchange.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    signal.signal(signal.SIGTERM, aggregation_filter.handle_sigterm)

    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.close_connections()

    return 0


if __name__ == "__main__":
    main()

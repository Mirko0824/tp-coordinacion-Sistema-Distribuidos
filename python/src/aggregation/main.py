import os
import logging
import bisect

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

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_fruit = self.clients_fruits.get(client_id, {})
        # Obtengo el fruitItem, si no existe devuelve un fruitItem con cantidad 0 y le suma el fruitItem con la cantidad recibida
        # Si ya existe la fruta, devuelve la cantidad que ya tenia, ignorando el fruitItem con cantidad 0 
        # y le suma el fruitItem con la cantidad recibida
        client_fruit[fruit] = client_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount)
        )
        # Guardo el top de frutas actualizado del client_id
        self.clients_fruits[client_id] = client_fruit

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        client_top = self.clients_fruits.get(client_id, {})
        # Obtengo el top de frutas ordenado por cantidad de mayor a menor
        fruit_top = sorted(client_top.values())
        fruit_top.reverse()
        # Obtengo el top de frutas limitado a TOP_SIZE
        fruit_top = fruit_top[:TOP_SIZE]
        # Guardo la fruta y la cantidad en una lista de tuplas para enviarlo a join
        fruit_list = []
        for fruit in fruit_top:
            fruit_list.append((fruit.fruit, fruit.amount))

        # Defino el json que envia a join con el top de frutas del client_id
        top_fruits_message = {
            "type": message_protocol.internal.MessageType.PARCIAL_TOP,
            "client_id": client_id,
            "top_fruits": fruit_list,
        }

        self.output_queue.send(message_protocol.internal.serialize(top_fruits_message))
        self.clients_fruits.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        message_type = fields.get("type")
        # Valido si el mensaje es de tipo data o eof
        if message_type == message_protocol.internal.MessageType.PARCIAL_SUM:
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        elif message_type == message_protocol.internal.MessageType.EOF_SUM:
            self._process_eof(fields["client_id"])
        
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

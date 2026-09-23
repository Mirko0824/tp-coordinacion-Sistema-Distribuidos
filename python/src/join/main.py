import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.clients_parcial_tops = {}

    def _process_data(self, client_id, aggregation_id, parcial_top):
        
        client_tops = self.clients_parcial_tops.setdefault(client_id, {})
        client_tops[aggregation_id] = parcial_top

        # Verifico que todos los aggregations hayan enviado el top parcial
        if len(client_tops) < AGGREGATION_AMOUNT:
            return
        
        # Agrupo los top_parciales de los aggregations para obtener el top final
        fruits = []
        for aggregation_top in client_tops.values():
            for fruit, amount in aggregation_top:
                fruits.append(fruit_item.FruitItem(fruit, int(amount)))

        # Ordeno de mayor a menor y me quedo con los primeros TOP_SIZE
        final_top = sorted(fruits, reverse=True)[:TOP_SIZE]

        # Creo la lista con el top final para enviarlo al cliente
        fruit_list = [
            (fruit.fruit, fruit.amount)
            for fruit in final_top
        ]

        join_message = {
            "type": message_protocol.internal.MessageType.FINAL_TOP,
            "client_id": client_id,
            "top_fruits": fruit_list,
        }
        self.output_queue.send(message_protocol.internal.serialize(join_message))
        self.clients_parcial_tops.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        message_type = fields.get("type")
        client_id = fields.get("client_id")
        aggregation_id = fields.get("aggregation_id")
        parcial_top = fields.get("top_fruits")
        
        # Valido el type y envio el mensaje a la queue
        if message_type == message_protocol.internal.MessageType.PARCIAL_TOP:
            self._process_data(client_id, aggregation_id, parcial_top)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

import os
import logging
import threading

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

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.clients_fruit_sum[client_id] = self.clients_fruit_sum.get(client_id, {})

        # Si la fruta ya existe, sumo la cantidad, sino se agrega
        self.clients_fruit_sum[client_id][fruit] = self.clients_fruit_sum[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        # Obtengo las frutas y cantidades del client_id y si no encuentra devuelve json vacio
        client_fruits = self.clients_fruit_sum.get(client_id, {})
        # Recorro cada fruta del json para enviarlos cada uno a las instancias de aggregation
        # Envio todas las frutas y sumas parciales acumuladas del client_id
        for parcial_fruit in client_fruits.values():
            parcial_fruit_message = {
                "type": message_protocol.internal.MessageType.PARCIAL_SUM,
                "client_id": client_id,
                "fruit": parcial_fruit.fruit,
                "amount": parcial_fruit.amount,
            }
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(parcial_fruit_message)
                )

        eof_message = {
            "type": message_protocol.internal.MessageType.EOF_SUM,
            "client_id": client_id,
        }
        # Envio mensaje de eof para a todos los aggregations indicando que no hay mas resultados parciales del cliente
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize(eof_message))

        # Elimino todo el json del client_id
        self.clients_fruit_sum.pop(client_id, None)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        message_type = fields["type"]
        # Valido si el mensaje es de tipo data o eof
        if message_type == message_protocol.internal.MessageType.FRUIT_INFO:
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        elif message_type == message_protocol.internal.MessageType.EOF_CLIENT:
            self._process_eof(fields["client_id"])
        
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

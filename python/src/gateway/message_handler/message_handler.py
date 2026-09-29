import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        # Creo el client_id usando uuid4 para obtener un id unico y lo guardo como un string hexadecimal
        self.client_id = uuid.uuid4().hex
        # Cantidad de registros enviados por el cliente
        self.message_total_amount = 0
    
    def serialize_data_message(self, message):
        fruit, amount = message
        # Este id es para que se verifique que no se este aumentando el contador duplicado en el sum
        message_id = self.message_total_amount
        self.message_total_amount += 1
        client_message = {
            "type": message_protocol.internal.MessageType.FRUIT_INFO,
            "client_id": self.client_id,
            "message_id": message_id,
            "fruit": fruit,
            "amount": amount,
        }
        return message_protocol.internal.serialize(client_message)

    def serialize_eof_message(self, message):
        client_message = {
            "type": message_protocol.internal.MessageType.EOF_CLIENT,
            "client_id": self.client_id,
            "message_total_amount": self.message_total_amount,
        }
        return message_protocol.internal.serialize(client_message)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields.get("type") != message_protocol.internal.MessageType.FINAL_TOP:
            return None

        if fields.get("client_id") != self.client_id:
            return None
        
        return fields["top_fruits"]

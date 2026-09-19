import json

# Defino los tipos de mensajes que se envian
class MessageType:
    # Mensajes a sum con info de fruta y cantidad
    FRUIT_INFO = 0x01
    # Mensajes que va de sum a aggregation enviando la suma parcial de las frutas
    PARCIAL_SUM = 0x02
    # Mensajes que envian el aggregation a join con el top de frutas del cliente
    PARCIAL_TOP = 0x03
    # Mensajes que va desde aggregation a join enviando el top final de frutas del cliente
    FINAL_TOP = 0x04
    # Mensajes que va de sum a aggregation indicando fin de sumas parciales
    EOF_SUM = 0x05
    # Mensajes de gateway a sum indicando eof
    EOF_CLIENT = 0x06

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

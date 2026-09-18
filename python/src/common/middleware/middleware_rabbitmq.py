import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.consumer_connection = None
        self.producer_connection = None
        self.consumer_channel = None
        self.producer_channel = None

    def send(self, message):
        try:
            # Se inicializa la conexion con rabbitmq de parte del productor
            self.producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
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
            self.consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.consumer_channel = self.consumer_connection.channel()
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)

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
            # Empieza a recibir los mensajes de la cola
            self.consumer_channel.start_consuming()
        except Exception as error:
            raise MessageMiddlewareMessageError(error)

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
            if self.consumer_channel and self.consumer_channel.is_open:
                # Llamo el metodo de stop_consuming de pika para romper el ciclo infinito
                self.consumer_channel.stop_consuming()
        except Exception as error:
            # Levanto excepcion de conexion
            raise MessageMiddlewareDisconnectedError(error)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_connection = None
        self.producer_connection = None
        self.consumer_channel = None
        self.producer_channel = None

    def start_consuming(self, on_message_callback):
        try:
            # Inicializo conexion con rabbitmq y creo canal
            self.consumer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.consumer_channel = self.consumer_connection.channel()
        except Exception as error:
            # Levanto excepcion de conexion
            raise MessageMiddlewareDisconnectedError(error)
        
        try:
            # Defino el exchanger con el nombre y tipo 'direct' que busca coincidencia exacta
            self.consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

            # Creo la cola donde se encolan los mensajes, en el queue name no le paso nada y lo genera automaticamente rabbitmq
            result = self.consumer_channel.queue_declare(queue='', exclusive=True)
            # Guardo el nombre generado
            queue_name = result.method.queue

            # Recorro todos los routing_keys que son todos los tipos/claves de mensajes que quiero que se encolen
            for rk in self.routing_keys:
                self.consumer_channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=rk)
            
            # Defino la funcion callback clousure con las funciones ack y nack dentro
            def callback(channel, method, properties, body):
                ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                on_message_callback(body, ack, nack)

            # Defino de que cola quiero consumir, la funcion callback que llamo cada vez que entra un mensaje y empeizo a consumir
            self.consumer_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            self.consumer_channel.start_consuming()
        except Exception as error:
            # Levanto excepcion por algun error interno
            raise MessageMiddlewareMessageError(error)

    def send(self, message):
        try:
            self.producer_connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.producer_channel = self.producer_connection.channel()
        except Exception as error:
            # Levanto excepcion de conexion
            raise MessageMiddlewareDisconnectedError(error)
        
        try:
            self.producer_channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

            # Recorro todos los routing_keys y envio el mismo mensaje con cada clave diferente
            for rk in self.routing_keys:
                self.producer_channel.basic_publish(exchange=self.exchange_name, routing_key=rk, body=message)

        except Exception as error:
            # Levanto excepcion por algun error interno
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
            if self.consumer_channel and self.consumer_channel.is_open:
                # Llamo el metodo de stop_consuming de pika para romper el ciclo infinito
                self.consumer_channel.stop_consuming()
        except Exception as error:
            raise MessageMiddlewareDisconnectedError(error)
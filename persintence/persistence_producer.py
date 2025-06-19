import pika
import json
import time

def send_pensistece_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Fila NÃO durável (desaparece após restart)
    channel.queue_declare(queue='persistenece_queue', durable=True)
    
    message = {'data': 'Mensagem de persistência', 'timestamp': time.time()}
    
    channel.basic_publish(
        exchange='',
        routing_key='persistenece_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # 1 TRANSIENTE | 2 - PERSISTENTE
        )
    )
    
    connection.close()

if __name__ == '__main__':
    send_pensistece_message()
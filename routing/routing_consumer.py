import pika
import sys
import time
from datetime import datetime

def get_connection():
    """Cria uma conexão com RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    return connection

def direct_consumer():
    """Consumidor para direct exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    severities = ['info', 'warning', 'error']
    
    for severity in severities:
        channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key=severity)
    
    def callback(ch, method, properties, body):
        print(f"[x] {method.routing_key}: {body.decode()}")
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
    print(f'[*] Aguardando logs {severities}. Para sair pressione CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()


def topic_consumer():
    """Consumidor para topic exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange='topic_logs', queue=queue_name, routing_key=binding_key)

    print(' [*] Waiting for logs. To exit press CTRL+C')


    def callback(ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body}")


    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

def fanout_consumer():
    """Consumidor para fanout exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='notifications', exchange_type='fanout')
    
    # Cria uma fila temporária exclusiva
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Vincula a fila ao exchange
    channel.queue_bind(exchange='notifications', queue=queue_name)


    service_name = sys.argv[1:]
    if not service_name:
        sys.stderr.write("Usage: %s [service_name]...\n" % sys.argv[0])
        sys.exit(1)
    
    def callback(ch, method, properties, body):
        print(f"[{service_name}] Recebido: {body.decode()}")
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
    print(f'[{service_name}] Aguardando notificações...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    fanout_consumer()
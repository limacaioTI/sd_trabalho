import pika
import sys
import time
from datetime import datetime

def get_connection():
    """Cria uma conexão com RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    return connection

def direct_producer():
    """Produtor para direct exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    
    severities = ['info', 'warning', 'error']
    
    for severity in severities:
        message = f"Log {severity.upper()}: Mensagem de {severity} - {datetime.now()}"
        
        channel.basic_publish(
            exchange='direct_logs',
            routing_key=severity,
            body=message
        )
        
        print(f"[x] Enviado [{severity}] {message}")
        time.sleep(1)
    
    connection.close()

def topic_producer():
    """Produtor para topic exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    channel.basic_publish(
        exchange='topic_logs', routing_key=routing_key, body=message)
    print(f" [x] Sent {routing_key}:{message}")
    connection.close()

def fanout_producer():
    """Produtor para fanout exchange"""
    connection = get_connection()
    channel = connection.channel()
    
    channel.exchange_declare(exchange='notifications', exchange_type='fanout')
    
    message = f"Notificação broadcast - {datetime.now()}"
    
    channel.basic_publish(
        exchange='notifications',
        routing_key='',  # Routing key é ignorada no fanout
        body=message
    )
    
    print(f"[x] Enviado broadcast: {message}")
    connection.close()

if __name__ == '__main__':
    fanout_producer()
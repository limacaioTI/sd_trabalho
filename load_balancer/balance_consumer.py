import pika
import time
import threading
import random
from datetime import datetime
import json
import sys


def get_connection():
    """Cria uma conexão com RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    return connection


def round_robin_consumer():
    worker_id = sys.argv[1]

    connection = get_connection()
    channel = connection.channel()
    
    channel.queue_declare(queue='round_robin_tasks', durable=True)
    
    def callback(ch, method, properties, body):
        task = json.loads(body.decode())
        print(f"[Consumer {worker_id}] ✅ Concluído: {task['data']}")

    channel.basic_consume(queue='round_robin_tasks', on_message_callback=callback, auto_ack=True)
    
    print(f'[Consumer {worker_id}] 🚀 Aguardando tarefas...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()

def round_robin_consumer_available():
    worker_id = sys.argv[1]

    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(queue='round_robin_tasks', durable=True)
    
    def callback(ch, method, properties, body):
        task = json.loads(body.decode())
        print(f"[Consumer {worker_id}] Iniciando: {task['data']}")
        if(worker_id == "1"):
            time.sleep(10)
        print(f"[Consumer {worker_id}] ✅ Concluído: {task['data']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='round_robin_tasks', on_message_callback=callback)
    
    print(f'[Consumer {worker_id}] 🚀 Aguardando tarefas...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()


if __name__ == '__main__':
    weighted_round_robin_consumer()
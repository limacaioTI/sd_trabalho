import pika
import time
import threading
import random
from datetime import datetime
import json

def get_connection():
    """Cria uma conexão com RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    return connection

def round_robin_producer(num_tasks=6):
    connection = get_connection()
    channel = connection.channel()
    
    channel.queue_declare(queue='round_robin_tasks', durable=True)
    
    print(f"[*] Enviando {num_tasks} tarefas para balanceamento Round Robin")
    
    for i in range(num_tasks):
        
        task = {
            'id': i + 1,
            'data': f'Processamento Tarefa {i + 1}',
        }
        
        message = json.dumps(task)
        
        channel.basic_publish(
            exchange='',
            routing_key='round_robin_tasks',
            body=message,
        )
        
        print(f"[x] Enviado: Tarefa {i + 1}")
        time.sleep(1)
    
    connection.close()


if __name__ == '__main__':
    round_robin_producer()
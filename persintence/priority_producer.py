import pika
import sys
import time
import json

def get_connection():
    return pika.BlockingConnection(pika.ConnectionParameters('localhost'))

def priority_producer():
    channel_name = 'priority_queue'
    connection = get_connection()
    channel = connection.channel()
    
    channel.queue_declare(queue=channel_name, durable=True, arguments={'x-max-priority': 10})

    
    mensagens = [
        {'data': 'Mensagem prioridade baixa', 'priority': 1},
        {'data': 'Mensagem prioridade média', 'priority': 5},
        {'data': 'Mensagem prioridade alta', 'priority': 10},
        {'data': 'Mensagem prioridade baixa', 'priority': 3},
        {'data': 'Mensagem prioridade média', 'priority': 4},
        {'data': 'Mensagem prioridade alta', 'priority': 2},
        {'data': 'Mensagem prioridade baixa', 'priority': 9},
        {'data': 'Mensagem prioridade média', 'priority': 7},
        {'data': 'Mensagem prioridade alta', 'priority': 6},
        {'data': 'Mensagem prioridade alta', 'priority': 8},
    ]

    for msg in mensagens:
        channel.basic_publish(
            exchange='',
            routing_key=channel_name,
            body=json.dumps(msg),
            properties=pika.BasicProperties(priority=msg['priority'], delivery_mode=2)
        )
        print(f"[x] Enviado: {msg['data']} (prioridade {msg['priority']})")
        time.sleep(1)
    connection.close()

if __name__ == '__main__':
    priority_producer() 
import pika
import json
import time

def get_connection():
    return pika.BlockingConnection(pika.ConnectionParameters('localhost'))

def priority_consumer():
    channel_name = 'priority_queue'
    connection = get_connection()
    channel = connection.channel()
    
    channel.queue_declare(queue=channel_name, durable=True, arguments={'x-max-priority': 10})

    def callback(ch, method, properties, body):
        msg = json.loads(body)
        print(f"[x] RECEBIDO: {msg['data']} (prioridade {properties.priority})")
        time.sleep(2)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=channel_name, on_message_callback=callback, auto_ack=False)
    print('[*] Aguardando mensagens de prioridade. Para sair pressione CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    priority_consumer() 
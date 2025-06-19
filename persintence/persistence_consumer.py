import pika
import json
import time

def receive_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='persistenece_queue', durable=True)
    
    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"📥 RECEBIDO: {message['data']}")
        time.sleep(10)  # Simula processamento
        print("✅ Processado - enviando confirmação...")
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Confirma a recepção
    
    channel.basic_consume(
        queue='persistenece_queue',
        on_message_callback=callback,
        auto_ack=False  # Precisa de confirmação manual
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()

    

if __name__ == '__main__':
    receive_message()
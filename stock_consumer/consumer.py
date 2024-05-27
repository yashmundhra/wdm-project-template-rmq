import pika
import requests

STOCK_URL = "http://stock-service:5000"

# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue="task_queue", durable=True)


def add_stock(item_id: str):
    requests.post(f"{STOCK_URL}/consumed/{item_id}")


def callback(ch, method, properties, body):
    add_stock(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue="task_queue", on_message_callback=callback)
channel.start_consuming()

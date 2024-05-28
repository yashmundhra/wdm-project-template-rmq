import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests
import pika
import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.get('/find_item/<item_id>')
def find_item(item_id: str):
    app.logger.info(f"HEREE")
    # RabbitMQ setup
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    except pika.exceptions.AMQPConnectionError as exc:
        print("Failed to connect to RabbitMQ service. Message wont be sent.")
        return
    
    channel = connection.channel()
    # Declare queues
    channel.queue_declare(queue='find_item_queue')
    channel.queue_declare(queue='find_item_response_queue')

    message = json.dumps({'item_id': item_id})
    channel.basic_publish(exchange='',
                          routing_key='find_item_queue',
                          body=message)
    app.logger.info(f"Requested item: {item_id}")
    # Block and wait for the response
    connection.process_data_events(time_limit=None)
    return "idk bruh"

# channel.basic_consume(
#     queue="find_item_response_queue",
#     on_message_callback=on_find_item_response,
#     auto_ack=True,
# )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


# import logging
# import os
# import atexit
# import random
# import uuid
# import threading
# from collections import defaultdict

# import redis
# import requests
# import pika
# import json
# from msgspec import msgpack, Struct
# from flask import Flask, jsonify, abort, Response

# DB_ERROR_STR = "DB error"
# REQ_ERROR_STR = "Requests error"

# GATEWAY_URL = os.environ['GATEWAY_URL']

# app = Flask("order-service")

# db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
#                               port=int(os.environ['REDIS_PORT']),
#                               password=os.environ['REDIS_PASSWORD'],
#                               db=int(os.environ['REDIS_DB']))

# connection = None
# channel = None
# response_cache = {}

# def close_db_connection():
#     db.close()

# def close_rabbitmq_connection():
#     if connection and connection.is_open:
#         connection.close()

# atexit.register(close_db_connection)
# atexit.register(close_rabbitmq_connection)

# class OrderValue(Struct):
#     paid: bool
#     items: list[tuple[str, int]]
#     user_id: str
#     total_cost: int

# def get_order_from_db(order_id: str) -> OrderValue | None:
#     try:
#         # get serialized data
#         entry: bytes = db.get(order_id)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     # deserialize data if it exists else return null
#     entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
#     if entry is None:
#         # if order does not exist in the database; abort
#         abort(400, f"Order: {order_id} not found!")
#     return entry

# @app.get('/test_msq')
# def produce_message():
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
#     except pika.exceptions.AMQPConnectionError as exc:
#         print("Failed to connect to RabbitMQ service. Message wont be sent.")
#         return

#     channel = connection.channel()
#     channel.queue_declare(queue='task_queue', durable=True)
#     channel.basic_publish(
#         exchange='',
#         routing_key='task_queue',
#         body="Hello, Yash sent this message",
#         properties=pika.BasicProperties(
#             delivery_mode=2,  # make message persistent
#         ))

#     connection.close()
#     return "Sent Message"

# @app.post('/create/<user_id>')
# def create_order(user_id: str):
#     key = str(uuid.uuid4())
#     value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
#     try:
#         db.set(key, value)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({'order_id': key})

# @app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
# def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
#     n = int(n)
#     n_items = int(n_items)
#     n_users = int(n_users)
#     item_price = int(item_price)

#     def generate_entry() -> OrderValue:
#         user_id = random.randint(0, n_users - 1)
#         item1_id = random.randint(0, n_items - 1)
#         item2_id = random.randint(0, n_items - 1)
#         value = OrderValue(paid=False,
#                            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
#                            user_id=f"{user_id}",
#                            total_cost=2 * item_price)
#         return value

#     kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
#                                   for i in range(n)}
#     try:
#         db.mset(kv_pairs)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({"msg": "Batch init for orders successful"})

# @app.get('/find/<order_id>')
# def find_order(order_id: str):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     return jsonify(
#         {
#             "order_id": order_id,
#             "paid": order_entry.paid,
#             "items": order_entry.items,
#             "user_id": order_entry.user_id,
#             "total_cost": order_entry.total_cost
#         }
#     )

# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response

# def send_get_request(url: str):
#     try:
#         response = requests.get(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response

# @app.post('/addItem/<order_id>/<item_id>/<quantity>')
# def add_item(order_id: str, item_id: str, quantity: int):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
#     if item_reply.status_code != 200:
#         # Request failed because item does not exist
#         abort(400, f"Item: {item_id} does not exist!")
#     item_json: dict = item_reply.json()
#     order_entry.items.append((item_id, int(quantity)))
#     order_entry.total_cost += int(quantity) * item_json["price"]
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
#                     status=200)

# def rollback_stock(removed_items: list[tuple[str, int]]):
#     for item_id, quantity in removed_items:
#         send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

# @app.get('/find_item/<item_id>')
# def find_item(item_id: str):
#     app.logger.info(f"HEREE")
#     correlation_id = str(uuid.uuid4())
#     response_cache[correlation_id] = None

#     def on_response(ch, method, properties, body):
#         if properties.correlation_id == correlation_id:
#             response_cache[correlation_id] = json.loads(body)
#             app.logger.info(f"Received response: {response_cache[correlation_id]}")

#         # Publish request message
#         message = json.dumps({'item_id': item_id})
#         channel.basic_publish(
#             exchange='',
#             routing_key='find_item_queue',
#             body=message,
#             properties=pika.BasicProperties(
#                 reply_to='find_item_response_queue',
#                 correlation_id=correlation_id
#             )
#         )
#         app.logger.info(f"Requested item: {item_id}")

#         # Wait for the response
#         while response_cache[correlation_id] is None:
#             connection.process_data_events()

#         connection.close()
#         return jsonify(response_cache[correlation_id])

# def on_find_item_response(ch, method, properties, body):
#     response = json.loads(body)
#     correlation_id = properties.correlation_id
#     response_cache[correlation_id] = response

# def setup_rabbitmq():
#     global connection, channel
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
#         channel = connection.channel()
#         # Declare queues
#         channel.queue_declare(queue='find_item_queue')
#         channel.queue_declare(queue='find_item_response_queue')
#     except pika.exceptions.AMQPConnectionError as e:
#         app.logger.error(f"Failed to connect to RabbitMQ: {e}")

# def consume_messages():
#     try:
#         channel.basic_consume(
#             queue="find_item_response_queue",
#             on_message_callback=on_find_item_response,
#             auto_ack=True,
#         )
#         app.logger.info("Starting RabbitMQ consumer")
#         channel.start_consuming()
#     except Exception as e:
#         app.logger.error(f"Error in RabbitMQ consumer: {e}")
#         if connection and connection.is_open:
#             connection.close()

# @app.post('/checkout/<order_id>')
# def checkout(order_id: str):
#     app.logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     # get the quantity per item
#     items_quantities: dict[str, int] = defaultdict(int)
#     for item_id, quantity in order_entry.items:
#         items_quantities[item_id] += quantity
#     # The removed items will contain the items that we already have successfully subtracted stock from
#     # for rollback purposes.
#     removed_items: list[tuple[str, int]] = []
#     for item_id, quantity in items_quantities.items():
#         stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
#         if stock_reply.status_code != 200:
#             # If one item does not have enough stock we need to rollback
#             rollback_stock(removed_items)
#             abort(400, f'Out of stock on item_id: {item_id}')
#         removed_items.append((item_id, quantity))
#     user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
#     if user_reply.status_code != 200:
#         # If the user does not have enough credit we need to rollback all the item stock subtractions
#         rollback_stock(removed_items)
#         abort(400, "User out of credit")
#     order_entry.paid = True
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     app.logger.debug("Checkout successful")
#     return Response("Checkout successful", status=200)

# if __name__ == '__main__':
#      # Start Flask app in a separate thread
#     app.run(host="0.0.0.0", port=8000, debug=True)

#     flask_thread1 = threading.Thread(
#         # Setup RabbitMQ connection and channel
#         setup_rabbitmq()
#     )
#     flask_thread1.start()

#     flask_thread2 = threading.Thread(
#         # Start RabbitMQ consumer in a separate thread
#         consume_messages()
#     )
#     flask_thread2.start()
# else:
#     gunicorn_logger = logging.getLogger("gunicorn.error")
#     app.logger.handlers = gunicorn_logger.handlers
#     app.logger.setLevel(gunicorn_logger.level)
#     # Setup RabbitMQ connection and channel
#     setup_rabbitmq()
#     # Start RabbitMQ consumer in a separate thread
#     consumer_thread = threading.Thread(target=consume_messages)
#     consumer_thread.start()

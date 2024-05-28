import logging
import os
import atexit
import uuid
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import pika
import json

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

def close_db_connection():
    db.close()


def close_rabbitmq_connection():
    global connection
    if connection and connection.is_open:
        connection.close()


atexit.register(close_db_connection)
atexit.register(close_rabbitmq_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def on_find_item_request(ch, method, properties, body):
    app.logger.info("Received item request")
    request = json.loads(body)
    item_id = request["item_id"]
    try:
        item_entry = get_item_from_db(item_id)
        response = {
            "status": "success",
            "item": {
                "item_id": item_id,
                "stock": item_entry.stock,
                "price": item_entry.price,
            },
        }
    except Exception as e:
        response = {"status": "error", "message": str(e)}

    channel.basic_publish(
        exchange="", routing_key="find_item_response_queue", body=json.dumps(response)
    )
    app.logger.debug(f"Processed item request for: {item_id}")

    channel.stop_consuming()
    # return response


def setup_rabbitmq():
    app.logger.info("Setting up RabbitMQ connection")
    global connection, channel
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq", blocked_connection_timeout=300))
        channel = connection.channel()
        # Declare queues
        channel.queue_declare(queue="find_item_queue")
        channel.queue_declare(queue="find_item_response_queue")
    except pika.exceptions.AMQPConnectionError as e:
        app.logger.error(f"Failed to connect to RabbitMQ: {e}")


def consume_messages():
    app.logger.info("Consuming messages from RabbitMQ")
    try:
        channel.basic_consume(
            queue="find_item_queue",
            on_message_callback=on_find_item_request,
            auto_ack=True,
        )
        app.logger.info("Starting RabbitMQ consumer")
        channel.start_consuming()
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ consumer: {e}")
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    # Setup RabbitMQ connection and channel
    producer_thread = threading.Thread(target=setup_rabbitmq)
    producer_thread.start()

    # Start RabbitMQ consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Start Flask app in the main thread
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.info("Starting stock service1")

else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    # Setup RabbitMQ connection and channel
    setup_rabbitmq()
    # Start RabbitMQ consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()
    app.logger.info("Starting stock service2")


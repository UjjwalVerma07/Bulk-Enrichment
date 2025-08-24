from kafka import KafkaProducer,KafkaConsumer;
import json,os
KAFKA_BROKER=os.getenv("KAFKA_BROKER","kafka:9092")
# def get_producer():
#     producer=KafkaProducer(bootstrap_servers=KAFKA_BROKER,
#                            value_serializer=lambda v:json.dumps(v).encode("utf-8"))
#     return producer

import time
from kafka.errors import NoBrokersAvailable

def get_producer(retries=5, delay=2):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise NoBrokersAvailable(f"Could not connect to Kafka broker at {KAFKA_BROKER}")

def send_event(topic,event_data):
    producer=get_producer()
    producer.send(topic,event_data)
    producer.flush()
    print(f"Sent event to topic '{topic}': {event_data}")
    producer.close()

def consume_records(topic:str):
    consumer=KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    for message in consumer:
        yield message.value
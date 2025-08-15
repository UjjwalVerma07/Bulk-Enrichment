from kafka import KafkaProducer
import json,os
KAFKA_BROKER=os.getenv("KAFKA_BROKER","kafka:9092")

def get_producer():
    producer=KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                           value_serializer=lambda v:json.dumps(v).encode("utf-8"))
    return producer

def send_event(topic,event_data):
    producer=get_producer()
    producer.send(topic,event_data)
    producer.flush()
    print(f"Sent event to topic '{topic}': {event_data}")
    producer.close()


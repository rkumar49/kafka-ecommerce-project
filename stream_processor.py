import json
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS="host.docker.internal:29092"
INPUT_TOPIC="raw_events"
OUTPUT_TOPIC="clean_events"
GROUP_ID="silver-stream-processor"

VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # Manual offset management
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def is_valid_event(event):
    """Check if event passes all validation rules"""
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    if event.get("amount") is None or event.get("amount") <= 0:
        return False
    if not event.get("currency"):
        return False
    if event.get("is_valid") is not True:
        return False
    return True

print("Starting Silver Stream Processor....")

for message in consumer:
    key = message.key
    event = message.value
    
    if is_valid_event(event):
        producer.send(
            topic=OUTPUT_TOPIC,
            key=key,
            value=event
        )
        print(f"FORWARDED | key={key} | event_type={event['event_type']}")
    else:
        print(f"DROPPED | key={key} | reason=invalid")
    
    consumer.commit()  # Commit offset after processing
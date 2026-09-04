from kafka import KafkaProducer
import json
import socket

def test_connection(bootstrap_server):
    try:
        print(f"\nTrying: {bootstrap_server}")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000,
            max_block_ms=10000
        )
        # Try to get cluster metadata
        producer.send("test", value={"test": "message"})
        producer.flush()
        print(f"✅ SUCCESS! Connected to {bootstrap_server}")
        return True
    except Exception as e:
        print(f"❌ Failed: {str(e)[:100]}")
        return False

# Test all possibilities
servers = [
    "localhost:9092",
    "127.0.0.1:9092",
    "host.docker.internal:29092",
    "localhost:29092"
]

print("Testing Kafka connections...")
for server in servers:
    if test_connection(server):
        print(f"\n🎯 USE THIS: BOOTSTRAP_SERVERS = \"{server}\"")
        break
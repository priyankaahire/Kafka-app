from kafka import KafkaConsumer
import json

# Kafka cluster connection details
BOOTSTRAP_SERVERS = ['localhost:9092'] # Kafka broker address
TOPIC_NAME = "test_topic"              # Name of the Kafka topic to produce messages to

# Function: Consume messages from Kafka topic continuously
def run_consumer():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest', #Start from earliest if no offset
        enable_auto_commit=True,
        group_id="consumer-group-a", # Consumer group name
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) # Convert JSON to dict
    )

    print("Consumer started...")
    for message in consumer:
        print(f"Received:{message.value}, "
              f"partition={message.partition}, offset={message.offset}")

if __name__ == "__main__":
    run_consumer() 
    
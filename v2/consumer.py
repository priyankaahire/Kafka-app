from kafka import KafkaConsumer
import json

# Kafka cluster connection details
BOOTSTRAP_SERVERS = "kafka-demo-psahire01-aaed.k.aivencloud.com:15905" #['localhost:9092'] # Kafka broker address
TOPIC_NAME = "test_topic" 
SECURITY_PROTOCOL = "SSL"  

# Absolute certificate file paths
folderPath = "certs/"
SSL_CAFILE = folderPath+"ca.pem"
SSL_CERTFILE = folderPath+"service.cert"
SSL_KEYFILE = folderPath+"service.key"

# Function: Consume messages from Kafka topic continuously
def run_consumer():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        ssl_cafile=SSL_CAFILE,
        ssl_certfile=SSL_CERTFILE,
        ssl_keyfile=SSL_KEYFILE,
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
    
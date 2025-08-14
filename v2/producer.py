from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
from data_faker import get_registered_user
import time
from kafka.errors import TopicAlreadyExistsError
import os

# Kafka cluster connection details
# Kafka cluster connection details
BOOTSTRAP_SERVERS = "kafka-demo-psahire01-aaed.k.aivencloud.com:15905" #['localhost:9092'] # Kafka broker address
TOPIC_NAME = "test_topic"              # Name of the Kafka topic to produce messages to
PARTITIONS = 3                         # Number of partitions for the topic
REPLICATION_FACTOR = 5                 # Number of replicas for fault tolerance, must be <= number of brokers
SECURITY_PROTOCOL = "SSL"


# Absolute certificate file paths
folderPath = "certs/"
SSL_CAFILE = folderPath+"ca.pem"
SSL_CERTFILE = folderPath+"service.cert"
SSL_KEYFILE = folderPath+"service.key"

# useing bootsrap server to connect the kafka cluster. using value serializer when we want to send the 
# message to the kafak it will be send in serialize way

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Function: Create Kafka topic if it doesn't already exist
def craete_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        ssl_cafile=SSL_CAFILE,
        ssl_certfile=SSL_CERTFILE,
        ssl_keyfile=SSL_KEYFILE)
    existing_topics = admin_client.list_topics()
        
    if TOPIC_NAME not in existing_topics:
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        admin_client.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' created with {PARTITIONS} partitions and replication factor {REPLICATION_FACTOR}.")
    else:
        print(f"Topic '{TOPIC_NAME}' already exists.")

    admin_client.close()

# Function: Produce messages to Kafka topic continuously
def run_producer():
    # craete_topic()

    # Initialize Kafka producer with JSON serializer (Kafka can only send messages as bytes.)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        ssl_cafile=SSL_CAFILE,
        ssl_certfile=SSL_CERTFILE,
        ssl_keyfile=SSL_KEYFILE,
        api_version=(2, 8, 1),  # Match the Kafka version in Aiven console
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        while True:
            # message = {"message": "Hello from Aiven Kafka!"}
            # print(f"Producing: {message}")
            # producer.send(TOPIC_NAME, key={"key": 1}, value=message)
       
            registered_user = get_registered_user()
            print(f"Producing message: {registered_user}")

            # Send data to Kafka topic (can also pass a key if needed)
            producer.send(TOPIC_NAME, value=registered_user) 
            # producer.flush()

            # Wait 4 seconds before sending next message
            time.sleep(4) 
           
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        producer.close()
   

# Run the producer script
if __name__ == "__main__":
    run_producer()

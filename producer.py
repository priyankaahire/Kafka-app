from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
from data import get_registered_user
import time

# Kafka cluster connection details
BOOTSTRAP_SERVERS = ['localhost:9092'] # Kafka broker address
TOPIC_NAME = "test_topic"              # Name of the Kafka topic to produce messages to
PARTITIONS = 3                         # Number of partitions for the topic
REPLICATION_FACTOR = 1                 # Number of replicas for fault tolerance, must be <= number of brokers


# useing bootsrap server to connect the kafka cluster. using value serializer when we want to send the 
# message to the kafak it will be send in serialize way

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Function: Create Kafka topic if it doesn't already exist
def craete_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
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
    craete_topic()

    # Initialize Kafka producer with JSON serializer (Kafka can only send messages as bytes.)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    while True:
        registered_user = get_registered_user()
        print(f"Producing message: {registered_user}")

        # Send data to Kafka topic (can also pass a key if needed)
        producer.send(TOPIC_NAME, value=registered_user) 

        # Wait 4 seconds before sending next message
        time.sleep(4) 

# Run the producer script
if __name__ == "__main__":
    run_producer()

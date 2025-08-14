## Installation

This kafka demo app is relying on Faker and kafka-python which requiring Python 3.5 and above. The installation can be done via

pip install -r requirements.txt

# Kafka Producer & Consumer with Aiven

This project demonstrates how to use **Kafka Producer** and **Consumer** in Python with a Kafka cluster hosted on **Aiven**.

---

## 📌 Prerequisites

- Python 3.8+
- An Aiven Kafka service (created via [Aiven Console](https://console.aiven.io/))
- `requirements.txt` dependencies installed:
  ```bash
  pip install -r requirements.txt



## Getting Kafka Connection Details from Aiven
1. Log in to the Aiven Console.
2. Select your Kafka service.
3. Go to "Connection Information" → download the service.key, service.cert, and ca.pem files.
4. Copy them to your project directory:

## Configuration
Create a .env file or set environment variables:

KAFKA_BOOTSTRAP_SERVERS=<your-service-hostname>:<port>
KAFKA_TOPIC=my-topic
KAFKA_SSL_CA=certs/ca.pem
KAFKA_SSL_CERT=certs/service.cert
KAFKA_SSL_KEY=certs/service.key

## Running the Producer
python3 producer.py

## Running the Consumer
python3 consumer.py




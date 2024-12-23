import json
from kafka import KafkaProducer
import time


#to install kafka in python
#pip install git+https://github.com/dpkp/kafka-python.git

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test2"

JSONL_FILE_PATH = "boulder_flood_geolocated_tweets.json"

def read_jsonl(file_path, batch_size=10):

    with open(file_path, 'r', encoding='utf-8') as file:
        batch = []
        for line in file:
            try:
                record = json.loads(line.strip())
                batch.append(record)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {line.strip()}\nError: {e}")
        
        if batch:
            yield batch

def send_to_kafka(producer, topic, batch):
 
    for record in batch:
        producer.send(topic, value=record)
        print(f"Sent to Kafka: {record}")

def main():
 
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Connected to Kafka broker.")
    
    try:
        for batch in read_jsonl(JSONL_FILE_PATH, batch_size=10):
            send_to_kafka(producer, KAFKA_TOPIC, batch)
            time.sleep(10)  
    except KeyboardInterrupt:
        print("Stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    main()

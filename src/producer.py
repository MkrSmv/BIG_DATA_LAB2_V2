import csv
import time
import logging
import config
from kafka_clients import KafkaIoTProducer

logging.basicConfig(level=logging.INFO)

def run_producer():
    producer = KafkaIoTProducer(config.bootstrap_servers)
    logging.info(f"Connected to Kafka: {config.bootstrap_servers}")

    try:
        with open(config.iot_data_path, 'r') as file:
            reader = csv.DictReader(file)
            
            for count, row in enumerate(reader, start=1):
                # Send dict mapping
                producer.produce_iot_data(config.topic_data, row)
                
                if count % 100 == 0:
                    logging.info(f"Produced {count} records...")
                    producer.flush()
                
                # Simulate real-time streaming with a slight random delay
                time.sleep(0.05)
                
    except FileNotFoundError:
        logging.error(f"Dataset not found at {config.iot_data_path}. Please generate it first.")
    except KeyboardInterrupt:
        logging.info("Producer interrupted.")
    finally:
        producer.flush()
        logging.info("Producer shutdown.")

if __name__ == "__main__":
    # Wait for kafka and topic creation
    logging.info("Producer waiting to start...")
    time.sleep(15) 
    run_producer()

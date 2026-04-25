from confluent_kafka.admin import AdminClient, NewTopic
import config
import time
import logging

logging.basicConfig(level=logging.INFO)

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': config.bootstrap_servers})
    
    topics_to_create = [
        NewTopic(config.topic_data, num_partitions=2, replication_factor=2),
        NewTopic(config.topic_train_metrics, num_partitions=2, replication_factor=2)
    ]
    
    # Wait for brokers to be available by fetching metadata
    retries = 10
    while retries > 0:
        try:
            admin_client.list_topics(timeout=5.0)
            logging.info("Connected to Kafka brokers.")
            break
        except Exception as e:
            logging.warning(f"Waiting for Kafka brokers... {e}")
            retries -= 1
            time.sleep(3)
            
    if retries == 0:
        logging.error("Failed to connect to Kafka brokers. Exiting.")
        return

    fs = admin_client.create_topics(topics_to_create)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.info(f"Topic {topic} created successfully with replication_factor=2, num_partitions=2")
        except Exception as e:
            # Continue if topic already exists
            if "Topic_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                logging.info(f"Topic {topic} already exists.")
            else:
                logging.error(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    time.sleep(10) # Give kafka cluster some time to form
    create_topics()

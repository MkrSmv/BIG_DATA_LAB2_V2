from confluent_kafka import Producer, Consumer
import json
import logging

class BaseKafkaProducer:
    def __init__(self, bootstrap_servers):
        self.conf = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.conf)

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"Message delivery failed: {err}")

    def produce_message(self, topic, key, value_dict):
        try:
            self.producer.produce(
                topic,
                key=str(key),
                value=json.dumps(value_dict),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logging.error(f"Failed to produce message: {e}")

    def flush(self):
        self.producer.flush()


class BaseKafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, topics):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe(topics)

    def poll_message(self, timeout=1.0):
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            return None
        
        try:
            value = json.loads(msg.value().decode('utf-8'))
            return value
        except Exception as e:
            logging.error(f"Error decoding message: {e}")
            return None

    def close(self):
        self.consumer.close()


class KafkaIoTProducer(BaseKafkaProducer):
    def produce_iot_data(self, topic, data_row):
        self.produce_message(topic, key="iot_sensor_data", value_dict=data_row)


class KafkaMetricsProducer(BaseKafkaProducer):
    def produce_metrics(self, topic, iteration, metrics_dict):
        payload = {'iteration': iteration, **metrics_dict}
        self.produce_message(topic, key="model_metrics", value_dict=payload)


class KafkaDataConsumer(BaseKafkaConsumer):
    def __init__(self, bootstrap_servers, topics):
        super().__init__(bootstrap_servers, 'iot_data_consumer_group', topics)


class KafkaMetricsConsumer(BaseKafkaConsumer):
    def __init__(self, bootstrap_servers, topics):
        super().__init__(bootstrap_servers, 'streamlit_metrics_consumer_group', topics)

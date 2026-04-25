# Kafka Brokers
bootstrap_servers_kafka0 = 'kafka-0:9095'
bootstrap_servers_kafka1 = 'kafka-1:9096'

bootstrap_servers = f"{bootstrap_servers_kafka0},{bootstrap_servers_kafka1}"

# Topic Definitions
topic_data = 'topic_data'
topic_train_metrics = 'training_metrics'

# Data definition
iot_data_path = 'data/iot_telemetry.csv'
batch_size = 50

# Features for classification
features = ['temperature', 'vibration', 'pressure', 'voltage']
target = 'is_anomaly'

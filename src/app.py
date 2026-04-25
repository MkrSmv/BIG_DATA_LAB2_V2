import streamlit as st
import time
import config
from kafka_clients import KafkaMetricsConsumer

st.set_page_config(page_title='IoT Anomaly Detection Metrics', layout='wide')

st.title('IoT Telemetry XGBoost Training Metrics')

# Use session state to store metrics
metrics = ['log_loss', 'rmse', 'mae', 'error', 'auc']
chart_containers = {}

for metric in metrics:
    if metric not in st.session_state:
        st.session_state[metric] = []
    
    st.subheader(f'Metric: {metric}')
    chart_containers[metric] = st.empty()

@st.cache_resource
def get_consumer():
    return KafkaMetricsConsumer(config.bootstrap_servers, [config.topic_train_metrics])

consumer = get_consumer()

placeholder = st.empty()

while True:
    message = consumer.poll_message(timeout=1.0)
    if message is None:
        time.sleep(0.1)
        continue

    # message is dict already handled by the OOP consumer
    for key, value in message.items():
        if key in metrics:
            st.session_state[key].append(value)

    # Update charts
    for metric in metrics:
        if len(st.session_state[metric]) > 0:
            chart_containers[metric].line_chart(st.session_state[metric])

    placeholder.text(f"Last updated at: {time.strftime('%H:%M:%S')} - Model iteration: {message.get('iteration', 'N/A')}")

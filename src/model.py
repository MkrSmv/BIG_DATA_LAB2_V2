import time
import json
import logging
import random
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
import config
from kafka_clients import KafkaDataConsumer, KafkaMetricsProducer

logging.basicConfig(level=logging.INFO)

metrics_producer = KafkaMetricsProducer(config.bootstrap_servers)

class KafkaCallback(xgb.callback.TrainingCallback):
    def after_iteration(self, model, epoch, evals_log):
        metrics = {}
        if 'logloss' in evals_log['eval']:
            metrics['log_loss'] = evals_log['eval']['logloss'][-1]
        if 'rmse' in evals_log['eval']:
            metrics['rmse'] = evals_log['eval']['rmse'][-1]
        if 'mae' in evals_log['eval']:
            metrics['mae'] = evals_log['eval']['mae'][-1]
        if 'error' in evals_log['eval']:
            metrics['error'] = evals_log['eval']['error'][-1]
        if 'auc' in evals_log['eval']:
            metrics['auc'] = evals_log['eval']['auc'][-1]

        metrics_producer.produce_metrics(config.topic_train_metrics, epoch, metrics)
        logging.info(f"Iteration {epoch} metrics sent to Kafka: {metrics}")
        time.sleep(0.5)
        return False

def run_model():
    consumer = KafkaDataConsumer(config.bootstrap_servers, [config.topic_data])
    logging.info("Model Consumer started.")

    params = {
        'objective': 'binary:logistic',
        'eval_metric': ['logloss', 'auc', 'rmse', 'mae', 'error'],
        'learning_rate': 0.1,
        'max_depth': 3
    }

    X_buffer = []
    y_buffer = []
    
    bst = None

    try:
        while True:
            data = consumer.poll_message(timeout=1.0)
            if data is None:
                continue

            try:
                # Extract features
                x_row = [float(data[f]) for f in config.features]
                y_row = int(data[config.target])
                
                X_buffer.append(x_row)
                y_buffer.append(y_row)
            except Exception as e:
                logging.error(f"Malformed data received: {e}")
                continue

            # Batch training
            if len(X_buffer) >= config.batch_size:
                X = np.array(X_buffer)
                y = np.array(y_buffer)

                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)
                dtrain = xgb.DMatrix(X_train, label=y_train)
                dtest = xgb.DMatrix(X_test, label=y_test)

                logging.info(f"Training on new batch of {len(X_buffer)} samples...")

                if bst is not None:
                    bst = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=1000,
                        xgb_model=bst,
                        evals=[(dtest, 'eval')],
                        early_stopping_rounds=10,
                        callbacks=[KafkaCallback()]
                    )
                else:
                    bst = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=1000,
                        evals=[(dtest, 'eval')],
                        early_stopping_rounds=10,
                        callbacks=[KafkaCallback()]
                    )

                X_buffer.clear()
                y_buffer.clear()

    except KeyboardInterrupt:
        logging.info("Model training interrupted.")
    finally:
        consumer.close()

if __name__ == "__main__":
    time.sleep(20) # Wait for init and producer to start
    run_model()

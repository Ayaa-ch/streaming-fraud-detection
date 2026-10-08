import json
import os
import random
import sys
import threading
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import cfg, get_int, project_path

BATCH_MIN = get_int("KAFKA_BATCH_MIN", 1)
BATCH_MAX = get_int("KAFKA_BATCH_MAX", 10)
SEND_INTERVAL = get_int("KAFKA_SEND_INTERVAL_SECONDS", 3)


class DataGenerator:
    index = 0

    def __init__(self, csv_path: str = None):
        path = Path(csv_path) if csv_path else project_path("TEST_CSV")
        if not path.is_file():
            path = project_path("TRAIN_CSV")
        self.df = pd.read_csv(path, index_col=0)
        print(self.df.columns)

    def generateTransactions(self):
        num = random.randint(BATCH_MIN, BATCH_MAX)
        messages = self.df[self.index: self.index + num].to_dict(orient='records')
        self.index += num
        return messages


class MyProducer:
    topic_name = cfg.KAFKA_TOPIC

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[cfg.KAFKA_BOOTSTRAP_SERVERS],
            client_id=cfg.KAFKA_CLIENT_ID,
            acks=1,
            retries=5,
            key_serializer=lambda a: json.dumps(a).encode('utf-8'),
            value_serializer=lambda b: json.dumps(b).encode('utf-8')
        )

    def produce_message(self, message):
        self.producer.send(self.topic_name, message)

    def send_data(self, messages, multi=True):
        # Use threading for concurrent message production
        if multi:
            threads = []
            for message in messages:
                thread = threading.Thread(target=self.produce_message, args=(message,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        else:
            future = self.producer.send(self.topic_name, value=message, key=message['txn_id'])
            self.producer.flush()

    def __del__(self):
        self.producer.close()


if __name__ == "__main__":
    print("Starting app...")
    dataGenerator = DataGenerator()
    producer = MyProducer()

    try:
        while True:
            temp_data = dataGenerator.generateTransactions()
            producer.send_data(temp_data)
            print(f'Sent {len(temp_data)} messages...')
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        exit()

import json
from kafka import KafkaProducer

class EventProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="clickstream"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

    def send(self, event):
        data = event.to_dict()
        json_str = json.dumps(data)
        payload = json_str.encode("utf-8")
        self.producer.send(self.topic, value = payload)

    def close(self):
        self.producer.flush()
        self.producer.close()
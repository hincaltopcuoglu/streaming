import json
from kafka import KafkaConsumer
from factory import EventFactory

class EventConsumer:
    def __init__(self, bootstrap_servers = "localhost:9092", topic = "clickstream", group_id = "stream-learners-test-2"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
                            topic,
                            bootstrap_servers=bootstrap_servers,
                            group_id=group_id,
                            auto_offset_reset="earliest",
                            enable_auto_commit=True
                            )

    
    def poll_one(self, as_object = False):
        records = self.consumer.poll(timeout_ms=1000)

        for partition, messages in records.items():
            if messages:
                msg = messages[0]
                text = msg.value.decode("utf-8")
                data = json.loads(text)
                
                if as_object:
                    return EventFactory.from_dict(data)
                else:
                    return data
        return None

    def close(self):
        self.consumer.close()
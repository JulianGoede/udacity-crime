import json
import logging
import time

from kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        import time
        with open(self.input_file) as f:
            for calls in json.load(f):
                message = self.as_bytes(calls)
                self.send(topic=self.topic, value=message, partition=4)
                time.sleep(1)

    def as_bytes(self, json_dict):
        return bytes(json.dumps(json_dict), encoding='utf-8')

import os

from crime.producer.producer_server import ProducerServer

BROADCAST_URL = os.getenv("BROADCAST_URLS", 'localhost:9092')
TOPIC = 'crime.police.call'
DATA_DIR = os.getenv("DATA_DIR", f'{os.path.abspath(os.path.dirname(__file__))}/data')
RADIO_CODE_JSON = f'{DATA_DIR}/police-department-calls-for-service.json'


class KafkaProducer:

    def __init__(self, topic: str, bootstrap_servers: str, client_id: str, input_file: str = RADIO_CODE_JSON):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.input_file = input_file

    @property
    def run(self):
        return ProducerServer(
            input_file=self.input_file,
            topic=self.topic,
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            api_version=(5, 2, 2)  # must match with the broker in the docker-compose.yml
        )

    def feed(self):
        self.run.generate_data()


if __name__ == "__main__":
    KafkaProducer(topic=TOPIC, bootstrap_servers=BROADCAST_URL, client_id='crime-kafka-producer').feed()

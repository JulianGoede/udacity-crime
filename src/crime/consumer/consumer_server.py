from typing import List
import json
import logging
import time

from confluent_kafka import Consumer, OFFSET_BEGINNING

logger = logging.getLogger(__name__)

from crime.producer.kafka_server import BROADCAST_URL, TOPIC
from tornado import gen

import tornado


def handle_json_message(message):
    print(json.loads(message.value()))


class ConsumerServer:
    def __init__(
            self,
            topic_name_pattern=TOPIC,
            broker_urls=BROADCAST_URL,
            message_handler=handle_json_message,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,

    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            'bootstrap.servers': broker_urls,
            'group.id': '0'
        }

        self.consumer = Consumer(self.broker_properties)
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            time.sleep(1)
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(timeout=self.consume_timeout)
        if message is None:
            logger.info("no message received for pattern %s", self.topic_name_pattern)
            return 0
        elif message.error():
            logger.error("error - failed to consume data")
            return 0
        else:
            self.message_handler(message)
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("Shutdown consumer")
        self.consumer.close()


if __name__ == '__main__':
    message_handler = lambda message: print(message.value())

    consumer = ConsumerServer()
    try:
        tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        consumer.close()

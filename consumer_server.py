from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType

from kafka_server import KAFKA_SERVER, TOPIC_NAME

import logging
logging.getLogger("pykafka.broker").setLevel('ERROR')

ZOOKEEPER_SERVER = "localhost:2181"

client = KafkaClient(hosts=KAFKA_SERVER)
topic = client.topics[TOPIC_NAME]
consumer = topic.get_balanced_consumer(
    consumer_group=b'test-consumer-server',
    auto_commit_enable=False,
    auto_offset_reset=OffsetType.EARLIEST,
    zookeeper_connect=ZOOKEEPER_SERVER
)

for message in consumer:
    if message is not None:
        print(message.offset, message.value)
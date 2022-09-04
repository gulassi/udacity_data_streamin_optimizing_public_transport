"""Producer base-class providing common utilites and functionality"""
from ensurepip import bootstrap
import logging
from os import stat
import time


from confluent_kafka import avro, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }
        self.admin_client_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092"
        }
        self.admin_client = AdminClient(self.admin_client_properties)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.debug("starting topic creation")
        new_topic = [NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
        status = self.admin_client.create_topics(new_topic)
        for topic, f in status.items():
            try:
                f.result()
                logger.info(f"topic {topic} created")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info("topic already exists")
                else:
                    logger.error(f"failed to create topic {self.topic_name}: {e.args[0]}")

        logger.debug("completed topic creation")


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.debug("stopping producer")
        self.producer.flush()
        logger.debug("producer stopped")


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

# Please complete the TODO items in the code.

from dataclasses import dataclass, field
import json
from datetime import datetime
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise3.purchases"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return ""


def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    start_time = datetime.utcnow()
    curr_iteration = 0
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        p.produce(topic_name,Purchase().serialize())
        
        p.flush() #makes synchronous can be turned on/off
        if curr_iteration % 1000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(f"Message sent: {curr_iteration} | Total Elapsed Seconds: {elapsed}")
        curr_iteration += 1


def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass


if __name__ == "__main__":
    main()

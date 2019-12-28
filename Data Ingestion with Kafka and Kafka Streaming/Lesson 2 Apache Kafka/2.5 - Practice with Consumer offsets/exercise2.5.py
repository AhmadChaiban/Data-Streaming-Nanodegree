# Please complete the TODO items in the code

import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # TODO: Set the auto offset reset to earliest
    #       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    c = Consumer(
        {
            ## Running without the auto offset reset set to earliest will delay the consumer message 
            ## reception significantly at times. It should be set to "earliest" in this case.
            ## Again. Depending on the situation and the desired result, a user would set this to earliest,   
            ## latest, or not set it at all.
            "bootstrap.servers": BROKER_URL,
            "group.id": "0-take2",
            "auto.offset.reset":"earliest"
        }
    )

    # TODO: Configure the on_assign callback
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.subscribe
    
    ## Need to tell kafka about the location of the offset commit. This is to make it work on the earliest
    ## every time. 
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: Set the partition offset to the beginning on every boot.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.on_assign
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.TopicPartition
    
    ## Offset beginning is better to use than just using 0, this is because if the messages were set
    ## to expire, 0 would not be the true beginning. That is why importing and using "OFFSET_BEGINNING"
    ## from the confluent kafka library works better. 
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING ## Setting the partition offset to earliest on every restart
    consumer.assign(partitions) ## Assigning the partition

    # TODO: Assign the consumer the partitions
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.assign


def main():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume("com.udacity.lesson2.exercise5.iterations"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.1)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()

# Please complete the TODO items in this code

import asyncio
import json

import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "exercise2"


def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below.
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/filestream_connector.html#filesource-connector
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,  # TODO
                "config": {
                    ## Defining the connector class
                    "connector.class": "FileStreamSource",  # TODO
                    ## Topic name. This topic contains logs and hence the name
                    "topic": "lesson4.sample2.logs",  # TODO
                    ## Telling Kafka how many tasks it should run on its server
                    "tasks.max": 1,  # TODO
                    ## Telling Kafka to look for log data in this location
                    "file": f"/tmp/{CONNECTOR_NAME}.log",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )
    
    ## NOTE: USE "Kafka-console-consumer" TO SEE THIS DATA STREAM THROUGH
    ## kafka-console-consumer --topic lesson4.sample2.logs --bootstrap-server localhost:9092 --from-beginning (udacity workspace)
    
    # Ensure a healthy response was given
    resp.raise_for_status()
    print("connector created successfully")


async def log():
    """Continually appends to the end of a file"""
    with open(f"/tmp/{CONNECTOR_NAME}.log", "w") as f:
        iteration = 0
        while True:
            f.write(f"log number {iteration}\n")
            f.flush()
            await asyncio.sleep(1.0)
            iteration += 1


async def log_task():
    """Runs the log task"""
    task = asyncio.create_task(log())
    configure_connector()
    await task


def run():
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    run()

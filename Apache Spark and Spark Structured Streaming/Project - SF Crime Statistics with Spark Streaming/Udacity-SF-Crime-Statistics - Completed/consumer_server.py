from confluent_kafka import Consumer
import asyncio

BROKER_URL = "localhost:58050"

## Defining an async consumer
async def consume(topic_name):
    asyncio.sleep(1)
    
    c = Consumer({
        "bootstrap.servers":BROKER_URL,
        "group.id":"stream0",
        "auto.offset.reset":"earliest"
    })
    
    ## Subscribing to the topic
    c.subscribe([topic_name])
    
    ## polling and returning the message from the topic 
    ## with conditions
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)
        
## Need to use the async function to run
asyncio.run(consume("com.udacity.sf.crime"))
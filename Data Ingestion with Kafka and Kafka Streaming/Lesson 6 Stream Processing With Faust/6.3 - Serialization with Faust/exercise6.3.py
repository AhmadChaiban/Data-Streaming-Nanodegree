# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json

import faust

## The model types that allow for deserialization are 
## being defined here.
@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int

## The same as above but without email
@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int

## Defining the Faust app, and topic, with the value_type
## being specified and the borker as well
app = faust.App("exercise3", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# TODO: Define an output topic for sanitized click events, without the user email
#
sanitized_topic = app.topic('com.udacity.streams.sanitized',
                            value_type = ClickEventSanitized,
                            key_type = str,)

## Allows for serializing in a specific format. Faust makes it a lot easier
@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        # TODO: Modify the incoming click event to remove the user email.
        #       Create and send a ClickEventSanitized object.
        #
        
        #
        # TODO: Send the data to the topic you created above.
        #       Make sure to set a key and value
        #
        ClickEvent_sanitized = ClickEventSanitized(
            timestamp= clickevent.timestamp,
            uri= clickevent.uri,
            number= clickevent.number,
        )
        await sanitized_topic.send(key=clickevent.uri,value = ClickEvent_sanitized)

if __name__ == "__main__":
    app.main()

## Run with python exercise6.3.py worker
## view stream with kafka-console-consumer --bootstrap-server localhost:9092
## --topic com.udacity.streams.sanitized (optional --from-beginning)
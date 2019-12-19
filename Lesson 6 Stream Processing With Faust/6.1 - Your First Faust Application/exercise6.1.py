# Please complete the TODO items in this code

import faust

#
# TODO: Create the faust app with a name and broker
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
#
## Defining the actual faust app. Giving it a name and a broker
app = faust.App(
    'hello-world-faust',
    broker = 'localhost:9092'
)

#
# TODO: Connect Faust to com.udacity.streams.clickevents
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
#
## Topic being defined/created through FAUST app 
topic = app.topic('com.udacity.streams.purchases')

#
# TODO: Provide an app agent to execute this function on topic event retrieval
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor
#
## topic gets assigned to the app agent
@app.agent(topic)
async def clickevent(clickevents):
    # TODO: Define the async for loop that iterates over clickevents
    #       See: https://faust.readthedocs.io/en/latest/userguide/agents.html#the-stream
    # TODO: Print each event inside the for loop
    async for clickevent in clickevents:
        print(clickevent)


if __name__ == "__main__":
    ## This takes the original app that was defined 
    ## in the beginning of this notebook
    app.main() 
    
## Running commands: 
## python exercise6.1.py worker

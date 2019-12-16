# Please complete the TODO items in the code

## Just some syntax on how to use the REST Proxy to get info on Topics, brokers and partitions. 

import json

import requests  ## Should look up this library


REST_PROXY_URL = "http://localhost:8082"


def get_topics():
    """Gets topics from REST Proxy"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics")  # TODO

    try:
        ## Comes from the python request library
        resp.raise_for_status()
    except:
        print(f"Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()


def get_topic(topic_name):
    """Get specific details on a topic"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")  # TODO

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get topics {json.dumps(resp.json(), indent=2)})")

    print("Fetched topics from Kafka:")
    ## Useful for getting configuration information
    print(json.dumps(resp.json(), indent=2)) ## json dumps to format it better for reading


def get_brokers():
    """Gets broker information"""
    # TODO See: https://docs.confluent.io/current/kafka-rest/api.html#get--brokers
    resp = requests.get(f"{REST_PROXY_URL}/brokers")  # TODO

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get brokers {json.dumps(resp.json(), indent=2)})")

    print("Fetched brokers from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_partitions(topic_name):
    """Prints partition information for a topic"""
    # TODO: Using the above endpoints as an example, list
    #       partitions for a given topic name using the API
    #
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics-(string-topic_name)-partitions
    
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")
    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get Partitions {json.dumps(resp.json(), indent=2)})")

    print("Fetched partitions from Kafka:")
    print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    topics = get_topics()
    get_topic(topics[0])
    get_brokers()
    get_partitions(topics[-1])

    
## Extra commands: Try to use kafka-topics --list --zookeeper localhost:2181 to list the topics 
## acquired from the function get_topcs()
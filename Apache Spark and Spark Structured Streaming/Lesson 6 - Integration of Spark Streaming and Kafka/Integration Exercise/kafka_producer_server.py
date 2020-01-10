from confluent_kafka import Producer
import json
import time

class ProducerServer(Producer):
    
    def __init__(self,input_file,topic,**kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        
    def generate_data(self):
        with open(self.input_file) as f:
            p = Producer({'bootstrap.servers':'http://localhost:9092'})
            for line in f:
                p.poll(0)
                message = self.dict_to_binary(line)
                self.produce(self.topic,message,callback=delivery_report)
                time.sleep(0.5)
            p.flush()
                
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
            
    def dict_to_binary(self,json_dict):
        return json.dumps(json_dict).encode('utf-8')
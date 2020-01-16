from kafka import KafkaProducer
import json
import sys
import time

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            for item in self.iterload(f): # Load JSON items from the input_file, one item at a time
                message = self.dict_to_binary(item)
                # TODO send the correct data
                self.send(self.topic, message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')

    def iterload(self, file):
        buffer = ""
        dec = json.JSONDecoder()
        for line in file:
            line = line.strip(" \n\r\t")
            if line == "[":
                buffer = ""
                continue
                
            buffer = buffer + line
            if line == "},":
                yield dec.raw_decode(buffer)
                buffer = ""
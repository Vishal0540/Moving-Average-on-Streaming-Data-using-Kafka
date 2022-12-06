import time 
import json 
import random 
from datetime import datetime
from kafka import KafkaProducer
import hashlib
import sys


import pandas as pd



with open('config.json','r') as f:
    config_data = json.load(f)





# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=config_data['SERVER'],
    value_serializer=serializer
)



data_ = pd.read_csv(config_data['CSV_NAME'])

#Reoving na values
data_.dropna(inplace=True)


#data Formatting
data_['Application Date'] = pd.to_datetime(data_['Application Date']) 
data_['Year'] = data_['Application Date'].dt.year

data_['Application Date'] = data_['Application Date'].astype(str)
data_ = data_[(data_.Year==2008) | (data_.Year == 2009)]

data_['UID'] = data_.index
#data Formatting



if __name__ == '__main__':
    for ind,row in data_.iterrows():
        

       

        dummy_message = row.to_dict()
        
        
        # Send it to our 'messages' topic
        print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(config_data['TOPIC'], dummy_message)
        
        # Sleep for a random number of seconds
        time_to_sleep = 5
        time.sleep(time_to_sleep)
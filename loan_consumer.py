import json 
from kafka import KafkaConsumer

import pandas as pd
import os

if __name__ == '__main__':

    
    with open('config.json','r') as f:
        config_data = json.load(f)

    num = config_data['MA_NUMBER'] #number of moving Average

    consumer = KafkaConsumer(
        config_data['TOPIC'],
        bootstrap_servers = config_data['SERVER'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
    )

    previous_data = []

    if not previous_data:

        try:
            print(f"Getting Last {num} Records from Database")

            data = pd.read_csv("Database.csv")


            df_last_n_record = data.tail(num)
            data_in_dict_form = df_last_n_record.to_dict('records')


            print(f"Last {len(data_in_dict_form)} records recieved from Database {data_in_dict_form}")
            previous_data = data_in_dict_form

        except:
            print("Error in Fetching records from database")

            previous_data = []
        #Fetch last 5 fomr databse  



    for message in consumer:
        message_rcvd = json.loads(message.value) 
        print(f"Previous Data size:{len(previous_data)}: {previous_data}")
        print()
        if previous_data and len(previous_data)>=num:
            sum_ = 0
            for p_data in previous_data[-num:]:
                sum_ += p_data["Risk_Score"]
            message_rcvd[f"MovingAverage{num}"] = sum_/num
            previous_data.pop(0)


        else:
            message_rcvd[f"MovingAverage{num}"] = ''

        previous_data.append(message_rcvd)
        df = pd.DataFrame([message_rcvd])

        if os.path.exists('Database.csv'):
            df.to_csv('Database.csv', mode='a', index=False,header=False)
        else:
            df.to_csv('Database.csv', mode='a', index=False)


        




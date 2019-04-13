from google.cloud import pubsub
import time

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    
    This is a simple client program used for testing purposes
    It is used to fetch items published by Weather_to_Google_PubSub.py
    
    Version: Traffic_PutSub_Client v0.01
    
"""

#topic = "projects/elene6889/topics/weather-topic"

subscriber = pubsub.SubscriberClient()


file = open("traffic-data.txt", "w")
file.write("Traffic_PutSub_Client v0.01\n")
file.close()

def callback(message):



    file_str = str(message.data) + '\n'
    file = open("traffic-data.txt", "a")
    file.write(file_str)
    file.close()


    print("{}".format(message))
    message.ack()


subscription = "projects/elene6889/subscriptions/traffic-sub"

future = subscriber.subscribe(subscription, callback)

print('Starting to Listen {}'.format(subscription))

while True:
    time.sleep(30)



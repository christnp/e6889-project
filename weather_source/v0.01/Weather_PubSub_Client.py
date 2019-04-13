from google.cloud import pubsub
import time
import json

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    
    This is a simple client program used for testing purposes
    It is used to fetch items published by Weather_to_Google_PubSub.py
    
    Version: Weather_PutSub_Client v0.01
    
"""

#topic = "projects/elene6889/topics/weather-topic"

subscriber = pubsub.SubscriberClient()


file = open("weather-data.txt", "w")
file.write("Weather_PutSub_Client v0.01\n")
file.close()

def callback(message):

    file_str = str(message.attributes.get('timecode')) + ','
    file_str += str(message.data) + ','
    file_str += str(message.attributes.get('precip')) + ','
    file_str += str(message.attributes.get('summary')) + ','
    file_str += str(message.attributes.get('temp')) + ','
    file_str += str(message.attributes.get('time')) + ','
    file_str += str(message.attributes.get('uv')) + ','
    file_str += str(message.attributes.get('wind')) + '\n'

    file = open("weather-data.txt", "a")
    file.write(file_str)
    file.close()

    print("{}".format(message))
    message.ack()



subscription = "projects/elene6889/subscriptions/weather-sub"

future = subscriber.subscribe(subscription, callback)

print('Starting to Listen {}'.format(subscription))

while True:
    time.sleep(30)



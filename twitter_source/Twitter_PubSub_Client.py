from google.cloud import pubsub
import time

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    
    This is a simple client program used for testing purposes
    It is used to fetch items published by Twitter_to_Google_PubSub.py
    
"""

topic = "projects/elene6889/topics/twitter-topic"

subscriber = pubsub.SubscriberClient()


def callback(message):
    print("{}".format(message))
    message.ack()


subscription = "projects/elene6889/subscriptions/twitter-sub"

future = subscriber.subscribe(subscription, callback)

print('Starting to Listen {}'.format(subscription))

while True:
    time.sleep(30)



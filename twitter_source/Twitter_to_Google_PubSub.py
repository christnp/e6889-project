import tweepy
from google.cloud import pubsub
import time

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    This python file establishes a streaming connection to twitter and forwards selected tweets to
    Google Cloud Pub/Sub to serve as a candidate source for the ELEN-E6889 final project
    - A streaming connection is established with Twitter that returns tweets that orginate from within a
      specified geographic bounding box.
    - These tweets are then pushed to the Google Cloud Platform as a Pub/Sub topic
    - Several things are worth noting:
        1) Using the standard API, only a small percentage of tweets (~6%) have a location that can be inferred
        2) The filter that is used will return tweets that fall outside of the specified bounding box
        3) If one picks a really populated place where there is something to tweet, you will get plenty of data.
        (for example, the number tweets this code returns will increase significantly when you look at
        Central Park during a sunny day or a large arena during a sporting event)

"""

# configure Google Cloud PubSub
topic = "projects/elene6889/topics/twitter-topic"
publisher = pubsub.PublisherClient()

# create class to support streaming Twitter connection
# override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        #print(status.text)
        #print(status.created_at)
        tweet_dtc = str(status.created_at)
        #print(status.coordinates)

        #print("contributors:", status.contributors)
        #print("text:", status.text)
        #print("id:", status.id)
        #print("location:",status.user.location)
        #if str(status.coordinates) != "None":
            #print("coordinates:", status.coordinates)
        #if str(status.geo) != "None":
            #print("geo:", status.geo)
        #print("place:", status.place.full_name)
        #print("timestamp:", status.timestamp_ms)
        #print("created_at:", status.created_at)
        #print("author:", status.author)
        #print("user:", status.user)

        # publish Tweet to Google Cloud PubSub
        data = status.text
        #dtc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        publisher.publish(topic, data.encode('utf-8'), timecode=tweet_dtc)
        print("published tweet :", status.text)

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

# configure Twitter stream
auth = tweepy.OAuthHandler("0u0otpx8210Aq9xsOCk9nAZz9", "7EjAzixmA7Boncir8gwO3imPrzp4N18nYcpzPUaWm6igVw1d2G")
auth.set_access_token("1099110306591199233-nXHUGDsoz6VUqbtF1YG0m1wZd0JDVl" , "y7suFGLDoQkpGpOqefXZsPSWqSRCo9v4WUxNEIzjmEzfS")
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

# Columbia COSMOS Testbed
myStream.filter(locations=[-73.962266, 40.809294, -73.952029, 40.820262])

#myStream.filter(track=['nyc'])

#myStream.filter(locations=[-74.51426031557503,40.842381])
#NYC myStream.filter(locations=[-74.026675, 40.683935, -73.910408, 40.877483])

# Boston
#myStream.filter(locations=[-71.168146, 42.324009, -70.990103,42.427833])

# Union Square Park
#myStream.filter(locations=[-73.993613, 40.733973, -73.98739, 40.737909])

#myStream.api.search(place_id="68a6c55e55d5acc3")


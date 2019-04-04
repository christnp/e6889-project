import urllib.request, json
from google.cloud import pubsub
import time

"""
W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    This python file establishes a streaming connection to the Dark Sky weather API and forward current
    weather information to sGoogle Cloud Pub/Sub to serve as a candidate source for the ELEN-E6889 final project
    
    Weather info corresponds to a point at the center of the Columbia COSMOS testbed
    (40.814778, -73.957148) or (40°48'53.2"N 73°57'25.7"W)
    
    Dark Sky API is accessed using: 30fffcef194b5c2dcf808201da397c32
    
    The following is a sample API call that returns the current weather forecast:
    https://api.darksky.net/forecast/30fffcef194b5c2dcf808201da397c32/37.8267,-122.4233
    
    You are required to display the message “Powered by Dark Sky” that links to 
    https://darksky.net/poweredby/ somewhere prominent in your app or service. 
    
    # Sample output
    "latitude":40.814778,
    "longitude":-73.957148,
    "timezone":"America/New_York",
    "currently":
        "time":1554077279,
        "summary":"Mostly Cloudy",
        "icon":"partly-cloudy-night",
        "nearestStormDistance":40,
        "nearestStormBearing":294,
        "precipIntensity":0,
        "precipProbability":0,
        "temperature":45.64,
        "apparentTemperature":40.46,
        "dewPoint":27.07,
        "humidity":0.48,
        "pressure":1010.58,
        "windSpeed":10.49,
        "windGust":11.94,
        "windBearing":324,
        "cloudCover":0.69,
        "uvIndex":0,
        "visibility":9.3,
        "ozone":393.31},
        "offset":-4}
    
"""

# configure Google Cloud PubSub
topic = "projects/elene6889/topics/weather-topic"
publisher = pubsub.PublisherClient()

# Set Dark Sky weather API query string
base_query = "https://api.darksky.net/forecast/30fffcef194b5c2dcf808201da397c32/40.814778,-73.957148"
query_options = "?exclude=minutely,hourly,daily,alerts,flags"

while True:

    # Fetch current weather conditions
    r = urllib.request.urlopen(base_query+query_options)
    data_obj = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))

    # Google Cloud PubSub message
    weather_summary = str(data_obj["currently"]["icon"])
    #print(weather_summary)

    # Google Cloud PubSub attributes
    time_attr = str(data_obj["currently"]["time"])  # time
    summary_attr = str(data_obj["currently"]["summary"])  # summary
    temp_attr = str(data_obj["currently"]["temperature"])  # temperature - degrees F
    precip_attr = str(data_obj["currently"]["precipProbability"]) # precipProbability - %
    uv_attr = str(data_obj["currently"]["uvIndex"])  # uvIndex
    wind_attr = str(data_obj["currently"]["windSpeed"])  # windSpeed - mph

    # publish a message
    dtc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    publisher.publish(topic, weather_summary.encode('utf-8'), timecode=dtc, time=time_attr,
                      summary=summary_attr,precip=precip_attr,temp=temp_attr,uv=uv_attr,wind=wind_attr)

    print(weather_summary+":"+dtc)
    time.sleep(300)

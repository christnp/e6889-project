import logging
import numpy
import apache_beam as beam
import pandas
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import pubsub
from numpy import array

root = logging.getLogger()
root.setLevel(logging.INFO)

sdr_sub = "projects/elene6889/subscriptions/radio-sub"
tweet_sub = "projects/elene6889/subscriptions/twitter-sub"
image_sub = "projects/elene6889/subscriptions/traffic-sub"
weather_sub = "projects/elene6889/subscriptions/weather-sub"


# Create the Pipeline with the specified options.
argv = ["--streaming"]
options = PipelineOptions(flags=argv)

"""
# For Cloud execution, set the Cloud Platform project, job_name,
# staging location, temp_location and specify DataflowRunner.
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'elene6889'
google_cloud_options.job_name = 'final-project'
google_cloud_options.staging_location = 'gs://storage.kusmik.nyc/staging'
google_cloud_options.temp_location = 'gs://storage.kusmik.nyc/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

"""


def rain_num(x):
    return {
        'Clear': 0,
        'Partly Cloudy':0.1,
        'Mostly Cloudy':0.1,
        'Breezy and Mostly Cloudy': 0.1,
        'Overcast': 0.2,
        'Breezy and Overcast': 0.2,
        'Drizzle':0.4,
        'Light Rain': 0.6,
        'Rain': 0.8,
        'Heavy Rain': 1,
    }[x]

class PointFunction(beam.CombineFn):
    def create_accumulator(self):
        return (0.0,0)
    def add_input(self, sum_count, input):
        print("out")
        (sum, count) = sum_count
        return sum + input, count + 1
    def merge_accumplators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)
    def extract_output(self, sum_count):
        (sum, count) = sum_count
        return sum/count if count else float('NaN')


class valid_point(beam.DoFn):
    def process(self, element):

        df = pandas.DataFrame(element[1],columns=['key','value'])
        #logging.info("*** Valid Point =  %s", element)
        #logging.info("*** Valid Point =  %s", df.shape[0])
        if (df['key'].str.contains("tweets").any() and df['key'].str.contains("uv").any() and
            df['key'].str.contains("traffic").any()):

            logging.info("*** Valid Point")
            logging.info("*** Valid Point =  %s", element)
            yield [(float(0.1), float(0.2), float(0.3), float(0.4), float(0.5), float(0.6))]
        else:
            return

class normalize_point(beam.DoFn):

    def process(self, element):

        # max/min values to normalize point
        max_tweets = 1
        min_tweets = 320
        min_temp = 44.035
        max_temp = 76.665
        min_uv = 0.0
        max_uv = 8.0
        min_wind = 0.23
        max_wind = 19.605
        image_min = 0.0362919
        image_max = 0.2750302

        window_duration = 0.5  # duration of windowed tweet count in minutes

        #logging.info("++Element++ %s", element)

        tweet = ((element[0][0] * 10 / window_duration)- min_tweets) / (max_tweets - min_tweets)
        precip = element[0][1]
        temp = (element[0][2] - min_temp) / (max_temp - min_temp)
        uv = (element[0][3] - min_uv) / (max_uv - min_uv)
        wind = (element[0][4] - min_wind) / (max_wind - min_wind)
        traffic = (element[0][5] - image_min) / (image_max - image_min)

        yield [(tweet, precip, temp, uv, wind, traffic)]

class assign_cluster(beam.DoFn):

    def process(self, element):

        #test_point = array([0.397, 0.1, 0.426, 0.0, 0.434, 0.541])
        test_point = numpy.array(element)

        # medoid points between 0 and 1
        medoids = array(
                [[0.102, 0.2, 0.43, 0.0, 0.297, 0.124],
                [0.248, 0.1, 0.346, 0.125, 0.173, 0.564],
                [0.491, 0.6, 0.568, 0.375, 0.355, 0.589],
                [0.529, 0.1, 0.526, 0.375, 0.446, 0.544],
                [0.473, 0.2, 0.434, 0.0, 0.118, 0.311],
                [0.611, 0.0, 0.672, 0.875, 0.418, 0.81],
                [0.397, 0.1, 0.426, 0.0, 0.434, 0.541]])

        # calc distance point is from every cluster
        dist0 = numpy.linalg.norm(test_point - medoids[0])
        dist1 = numpy.linalg.norm(test_point - medoids[1])
        dist2 = numpy.linalg.norm(test_point - medoids[2])
        dist3 = numpy.linalg.norm(test_point - medoids[3])
        dist4 = numpy.linalg.norm(test_point - medoids[4])
        dist5 = numpy.linalg.norm(test_point - medoids[5])
        dist6 = numpy.linalg.norm(test_point - medoids[6])

        # determine which cluster was closest
        dist = [dist0,dist1,dist2,dist3,dist4,dist5,dist6]
        cluster = numpy.where(dist == min(dist))[0]
        #logging.info("*** Cluster = %s", str(dist))

        yield ("cluster", cluster)


# Create the Pipeline with the specified options.
p = beam.Pipeline(options=options)

# PubSub data to feed clustering
# tweets - dimension [0], comes from twitter source subscription
# precip - dimension [1], comes from weather source subscription
# temp - dimension [2], comes from weather source subscription
# uv - dimension [3], comes from weather source subscription
# wind - dimension [4], comes from weather source subscription
# traffic - dimension [5], comes from traffic source subscription

# get stream from traffic camera image source (corresponds to index [5] of medoids)
traffic_stream = ( p | "Read Image" >> beam.io.ReadFromPubSub(subscription=image_sub, with_attributes=True)
                    | "Traffic Window" >> beam.WindowInto(window.SlidingWindows(45,5),
                                        accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
                    | 'Traffic Value' >> beam.ParDo(
                            #lambda (c): [(c.attributes['localdatetime'], float(c.attributes['cnn_output']))])
                            lambda (c): [("traffic", float(c.attributes['cnn_output']))])
                    | 'Twitter Mean' >> beam.combiners.Mean.PerKey())
                    #| 'Log Traffic Value' >> beam.ParDo(lambda (c): logging.info("++Traffic_Value++ %s",c)))

# get stream from twitter source (corresponds to index [0] of medoids)
twitter_stream = ( p | "Read Twitter" >> beam.io.ReadFromPubSub(subscription=tweet_sub, with_attributes=True)
                    | "Twitter Window" >> beam.WindowInto(window.SlidingWindows(45,5),
                                        accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
                    #| 'Log Twitter Value' >> beam.ParDo(lambda (c): logging.info("++Twitter_Value++ %s",c.data)))
                    | 'Twitter Value' >> beam.ParDo(lambda (c): [("tweets", 1)])
                    | 'Twitter Count' >> beam.combiners.Count.PerKey())
                    #| 'Log Twitter Value' >> beam.ParDo(lambda (c): logging.info("++Twitter_Value++ %s",c)))

# get stream from twitter source (corresponds to indices [1],[2],[3],[4] of medoids)
weather_stream = ( p | "Read Weather" >> beam.io.ReadFromPubSub(subscription=weather_sub, with_attributes=True)
                    | "Weather Window" >> beam.WindowInto(window.SlidingWindows(45,5),
                                        accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
                    #| 'Log Weather Message' >> beam.ParDo(lambda (c): logging.info("++Weather_Value++ %s",c.attributes))
                    | 'Weather Value' >> beam.ParDo(lambda (c):
                                [("precip", rain_num(c.attributes['summary'])),("temp", float(c.attributes['temp'])),
                              ("uv", float(c.attributes['uv'])),("wind", float(c.attributes['wind']))])
                    | 'Weather Mean' >> beam.combiners.Mean.PerKey())
                    #| 'Log Weather Value' >> beam.ParDo(lambda (c): logging.info("++Weather_Value++ %s",c)))

merged_stream = ( (traffic_stream, twitter_stream, weather_stream, ) | "Join" >> beam.Flatten())

observed_point = ( merged_stream
                    | "Window" >> beam.WindowInto(window.FixedWindows(60),
                                    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
                    | 'Add Key' >> beam.ParDo(lambda (c): [("key", c)])
                    | 'Group' >> beam.GroupByKey()
                    | "Calc Point" >> beam.ParDo(valid_point())
                    | "Normalize Point" >> beam.ParDo(normalize_point())
                    | "Assign Cluster" >> beam.ParDo(assign_cluster())
                    | 'Log Point' >> beam.ParDo(lambda (a,c): logging.info("*** Observed Cluster =  %s",c)))

result = p.run()
result.wait_until_finish()

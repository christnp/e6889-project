import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from os import listdir
from os.path import isfile, join

"""

W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

Final Project
    This python file creates a baseline beam pipeline to support the final project

    subscriptions:
        projects/elene6889/subscriptions/traffic-sub
        projects/elene6889/subscriptions/weather-sub
        projects/elene6889/subscriptions/twitter-sub

"""

root = logging.getLogger()
root.setLevel(logging.INFO)

class process_twitter_message(beam.DoFn):
    # localdatetime, tweet
    def process(self, element):
        #logging.info("*** Twitter: %s \n", element[22:].decode("utf-8"))
        #logging.info("*** Twitter: %s \n", element)

        # only process messages that start with datetime stamp
        if element[5] == '-':
            tweet = element[23:].decode("utf-8")
            # 10 minute window
            return [(str(element[1:16]), str(tweet))]
            #return [(str(element[1:17]),str(tweet))]

class process_traffic_message(beam.DoFn):
    # localdatetime,predict
    def process(self, element):
        #logging.info("*** Traffic: %s \n", element[17:])
        #logging.info("*** Traffic: %s \n", element)

        # only process messages that start with datetime stamp
        if element[4] == '-':
            # 10 minute window
            return [(str(element[0:15]), float(element[17:]))]
            #return [(str(element[0:16]),float(element[17:]))]

class process_weather_message(beam.DoFn):

        def process(self, element):
            def rain_num(x):
                #logging.info("*** rain_num: %s \n", x)
                return {
                    'Clear': 0,
                    'Partly Cloudy': 0.1,
                    'Mostly Cloudy': 0.1,
                    'Breezy and Mostly Cloudy': 0.1,
                    'Overcast': 0.2,
                    'Breezy and Overcast': 0.2,
                    'Drizzle': 0.4,
                    'Light Rain': 0.6,
                    'Rain': 0.8,
                    'Heavy Rain': 1,
                }[x]

            # only process messages that start with datetime stamp
            if element[4] == '-':
                # localdatetime,icon,summary,precip,temp,time,uv,wind
                data_list = str(element).split(',')
                #logging.info("*** Weather data: %s \n", data_list[3])

                precip = rain_num(data_list[3])
                temp = data_list[4]
                uv = data_list[6]
                wind = data_list[7]
                #logging.info("*** Wind: %s \n", wind)

                # 10 minute window
                return [(str(element[0:15]), (float(precip), float(temp), float(uv), float(wind)))]
                #return [(str(element[0:16]), float(precip), float(temp), float(uv), float(wind))]

class merge_weather_data(beam.DoFn):

    def process(self, element):
        #logging.info("*** Traffic: %s \n", element[17:])
        #logging.info("*** Weather: %s \n", element[1])
        precip = 0
        temp = 0
        uv = 0
        wind = 0
        num_tuples = len(element[1])
        for i in range(0,num_tuples):
            #logging.info("*** precip: %s", element[1][i][0])
            #logging.info("*** temp: %s", element[1][i][1])
            #logging.info("*** uv: %s", element[1][i][2])
            #logging.info("*** wind: %s \n", element[1][i][3])
            precip += element[1][i][0]
            temp += element[1][i][1]
            uv += element[1][i][2]
            wind += element[1][i][3]
        precip /= num_tuples
        temp /= num_tuples
        uv /= num_tuples
        wind /= num_tuples
        return [(element[0], (round(precip,6), round(temp,6), round(uv,6), round(wind,6)))]


class show_tuple(beam.DoFn):
    def process(self, element):
        logging.info("*** %s",element)


def printSize(PColl,PName):
    ( PColl
            | "Counting Lines for %s" % PName
                >> beam.CombineGlobally(beam.combiners.CountCombineFn())
            | "Print line count for %s" % PName
                >> beam.ParDo(lambda (c): logging.info("\nTotal Lines is %s = %s \n", PName,c))
    )


p = beam.Pipeline(options=PipelineOptions())

"""
messages = ( p | 'Create Simple'
                    >> beam.Create ["one","two","three"])
            )
"""

twitter_messages = (p
                    #| 'Twitter Messages' >> beam.io.ReadFromText("corpus/twitter/twitter-04-14.txt", skip_header_lines=1)
                    | 'Twitter Messages' >> beam.io.ReadFromText("corpus/twitter/twitter-*.txt", skip_header_lines=1)
                    | 'Twitter Tuples' >> beam.ParDo(process_twitter_message())
            )

traffic_messages = (p
                    #| 'Traffic Messages' >> beam.io.ReadFromText("corpus/traffic/traffic_model_outputs-0.txt", skip_header_lines=1)
                    | 'Traffic Messages' >> beam.io.ReadFromText("corpus/traffic/traffic_model_outputs-*.txt", skip_header_lines=1)
                    | 'Traffic Tuples' >> beam.ParDo(process_traffic_message())
            )

weather_messages = (p
                    #| 'Weather Messages' >> beam.io.ReadFromText("corpus/weather/weather-04-14.txt", skip_header_lines=1)
                    | 'Weather Messages' >> beam.io.ReadFromText("corpus/weather/weather-*.txt", skip_header_lines=1)
                    | 'Weather Tuples' >> beam.ParDo(process_weather_message())
            )

#twitter_count = (twitter_messages | "twitter_count" >> beam.CombineGlobally(beam.combiners.CountCombineFn()))
#weather_count = (weather_messages | "weather_count" >> beam.CombineGlobally(beam.combiners.CountCombineFn()))
#traffic_count = (traffic_messages | "traffic_count" >> beam.CombineGlobally(beam.combiners.CountCombineFn()))


twitter_keyed_count = (twitter_messages
                       | "twitter_count" >> beam.CombinePerKey(beam.combiners.CountCombineFn())
                    )

#traffic_keyed_count = (traffic_messages
#                       | "traffic_count" >> beam.CombinePerKey(beam.combiners.CountCombineFn())
#                       )
traffic_keyed_mean = (traffic_messages
                       | "traffic_mean" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                    )

#weather_keyed_count = (weather_messages
#                       | "weather_count" >> beam.CombinePerKey(AverageFn())
#                    )

weather_keyed_mean = (weather_messages
                        | "weather_key" >> beam.GroupByKey()
                        | "weather_mean" >> beam.ParDo(merge_weather_data())
                    )

output = ({'twitter': twitter_keyed_count, 'traffic': traffic_keyed_mean, 'weather': weather_keyed_mean }
            | beam.CoGroupByKey()
        )

#(twitter_count | "twitter out" >> beam.ParDo(lambda (c): logging.info("**** Twitter Count: %s",c)))
#(weather_count | "weather out" >> beam.ParDo(lambda (c): logging.info("**** Weather Count: %s",c)))
#(traffic_count | "traffic out" >> beam.ParDo(lambda (c): logging.info("**** Traffic Count: %s",c)))

#twitter_peek = (twitter_keyed_count | "twitter_peek" >> beam.ParDo(show_tuple()))
#traffic_peek = (traffic_keyed_mean | "traffic_peek" >> beam.ParDo(show_tuple()))
#weather_peek = (weather_keyed_mean | "weather_peek" >> beam.ParDo(show_tuple()))
#output_peek = (output | "output_peek" >> beam.ParDo(show_tuple()))

#printSize(twitter_messages, "Twitter")
#printSize(weather_messages, "Weather")
#printSize(traffic_messages, "Traffic")
#printSize(output, "Output")

# write results to text file
#(twitter_keyed_count | "twitter_sink" >> beam.io.WriteToText("output/twitter_counts.txt"))
#(traffic_keyed_count | "traffic_sink" >> beam.io.WriteToText("output/traffic_counts.txt"))
#(weather_keyed_count | "weather_sink" >> beam.io.WriteToText("output/weather_counts.txt"))
(output | "output_sink" >> beam.io.WriteToText("output/cluster_data.txt"))


result = p.run()
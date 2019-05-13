#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Streaming Insigts for Urban Life
# ELEN E6889, Spring 2019
# Columbia University, New York
# Author: Nick Christman, nc2677
# Author: Aldo Kusmik, wak2116
# Author: Rashad Barghouti, rb3074
#——————————————————————————————————————————————————————————————————————————————
"""Apache Beam pipeline for processing live streams from a NYC block.

This file contains an impelementation built using the Apache Beam framework to
process live, geo-specific data from four streaming sources:
    (1) software-defined radio (SDR) implementation using GNU Radio (GR),
    (2) the Twitter API,
    (3) Darksky.net, (weather data), and
    (4) NYC DOT (traffic camera images).
The streams contain data specific to a New York city block near the Columbia
University campus. The SDR application listens in on frequencies used by the
city's EMS, PD, and FD, extracts relevant statistics from them, and publishes
those data to a Google Cloud (GC) Pub/Sub topic. The other three applications
perform simialr tasks to publish data from their sources to GC Pub/Sub topics.
All published data are then pulled from GC via Pub/Sub subscriptions and
delivered as the live streams at the input of the Beam pipeline.

Assumptions:
  GNU-Radio
    1) Ingested data is in the following format
        {
            data: 'VALUE'
            attributes: {
                "timestamp":      "YYYY-MM-DDTHH:MM:SS.SSSSSSSSSZ" (RFC3339 UTC "Zulu" format)
                "localdatetime":  "YYYY-MM-DD HH:MM:SS"
                "center_freq":    "VALUE_IN_HZ"
                "sample_rate":    "VALUE_IN_SAMPLE_PER_SEC"
            }
        }
    2) A Write transform to a BigQuerySink accepts PCollections of dictionaries.

    References:
    1.  https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

  Other:
    TBD
"""

import os
import sys
import time
import json
import pytz
import argparse
import datetime
import logging

import apache_beam as beam # pylint: disable=import-error
from apache_beam import window # pylint: disable=import-error
from apache_beam.pvalue import AsIter # pylint: disable=import-error
from apache_beam.options.pipeline_options import PipelineOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import SetupOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import StandardOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import GoogleCloudOptions # pylint: disable=import-error
from apache_beam.transforms.core import Windowing # pylint: disable=import-error
from apache_beam.transforms.trigger import AfterProcessingTime # pylint: disable=import-error
from apache_beam.transforms.trigger import AccumulationMode # pylint: disable=import-error
from apache_beam.transforms.trigger import AfterWatermark # pylint: disable=import-error
from apache_beam.metrics.metric import Metrics # pylint: disable=import-error
# from apache_beam.transforms.window import FixedWindows # pylint: disable=import-error
# from apache_beam.transforms.window import SlidingWindows # pylint: disable=import-error

from google.cloud import pubsub_v1 # pylint: disable=import-error,no-name-in-module
from google.api_core.exceptions import AlreadyExists, NotFound

# GR definitions
DT_FORMAT = '%Y-%m-%d %H:%M:%S'
GC_DT_FORMAT = '%Y%m%d-%H%M%S'
PROJECT = 'elen-e6889'
GR_TOPIC = 'gnuradio'
GR_SUB = 'gnuradio-sub'
GR_SNAP = 'gnuradio-snap'
GR_TABLE = 'rf_samples'
DATASET = 'project_data'
BEAM_TOPIC = 'beam-top'
DFLOW_TEMP = 'gs://e6889-bucket/tmp/'
STAGE = 'gs://e6889-bucket/stage/'
RUNNER = 'DataflowRunner'
THRESHOLD = 1.0

# API-sources defs
#sdr_sub = "projects/elene6889/subscriptions/radio-sub"
#tweet_sub = "projects/elene6889/subscriptions/twitter-sub"
#image_sub = "projects/elene6889/subscriptions/traffic-sub"
#weather_sub = "projects/elene6889/subscriptions/weather-sub"

# For Cloud execution, set the Cloud Platform project, job_name,
# staging location, temp_location and specify DataflowRunner.
#google_cloud_options = options.view_as(GoogleCloudOptions)
#google_cloud_options.project = 'elene6889'
#google_cloud_options.job_name = 'final-project'
#google_cloud_options.staging_location = 'gs://storage.kusmik.nyc/staging'
#google_cloud_options.temp_location = 'gs://storage.kusmik.nyc/temp'
#options.view_as(StandardOptions).runner = 'DataflowRunner'


###############################################################################
# HELPER FUNCTIONS
#
# to calculate timestamp for google cloud applications (different than display
# format)
def gc_timestamp():
    # get current GMT date/time
    # nyc_datetime = datetime.datetime.now(datetime.timezone.utc).strftime(DT_FORMAT)

    # get Eastern Time and return time in desired format
    nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
    return nyc_datetime.strftime(GC_DT_FORMAT)

def json_encode(x):
  return json.dumps(x)

def json_decode(x):
  return json.loads(x)
# END HELPER FUNCTIONS

###############################################################################
# DoFns for API sources
#
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

# End API-sources DoFns

###############################################################################
# MAIN PROCESSING FUNCTION
#

def run(argv=None):
    # RB: add_help=False to enable explicitly adding --help argument
    #parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(add_help=False, fromfile_prefix_chars='@')
    parser.add_argument('--help', '-h', action='help', help=argparse.SUPPRESS)
    parser.add_argument('--project', '-p',
                        dest='project',
                        default=PROJECT,
                        help='Google project used for Pub/Sub and Dataflow. ' \
                          'This project mus be configured prior to executing ' \
                          'the pipeline. Default: \'elen-e6889\'')
    parser.add_argument('--gr-topic', '-g',
                        dest='gr_topic',
                        default=GR_TOPIC,
                        help='GNU Radio topic for subscribing. The input ' \
                          'topic must be configured prior to executing the ' \
                          'pipeline. '\
                          'Default: \'gnuradio\'')
    parser.add_argument('--subscription', '-b',
                        dest='gr_sub',
                        default=GR_SUB,
                        help='GNU Radio subscription path. If subscription ' \
                          'does not exist, it will be created before executing '\
                          'the pipeline. The topic will be created if it does not '\
                          'already exist. '\
                          'Default: \'gnuradio-sub\'')
    parser.add_argument('--output', '-o',
                        dest='bm_topic',
                        default=BEAM_TOPIC,
                        help='Output topic for publishing. The output topic ' \
                          'must be configured prior to executing the ' \
                          'pipleine. Default: \'output\'')
    parser.add_argument('--temp', '-t',
                        dest='dflow_temp',
                        default=DFLOW_TEMP,
                        help='Google dataflow temporary storage. Default: ' \
                              '\'gs://e6889-bucket/tmp/\'')
    parser.add_argument('--stage', '-s',
                        dest='stage',
                        default=STAGE,
                        help='Google dataflow staging arge. Default: ' \
                              '\'gs://e6889-bucket/stage/\'')
    parser.add_argument('--threshold', '-d',
                        dest='threshold',
                        default=THRESHOLD,
                        type=float,
                        help='Threshold for signal detection. Signals above '\
                             'the threshold are consider not noise. Default: '\
                              '\'1\'')
    parser.add_argument('--runner', '-r',
                        dest='runner',
                        default=RUNNER,
                        help='TBeam runner to be used. Default: '\
                              '\'Dataflowrunner\'')

    # RB: add parsing for pipeline_args to disable warnings about the
    # arguements file option and to enable loading of GC parameters
    args, pipeline_args = parser.parse_known_args()
    #args = parser.parse_args()
    project = args.project
    gr_topic = args.gr_topic
    gr_sub = args.gr_sub
    bm_topic = args.bm_topic
    dflow_temp = args.dflow_temp
    stage = args.stage
    threshold = args.threshold
    runner = args.runner

    #static
    out_sub = 'output-sub'


###############################################################################
# GOOGLE PUB/SUB CONFIG
#

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    grtopic_path = publisher.topic_path(project, gr_topic)
    bmtopic_path = publisher.topic_path(project, bm_topic)
    grsub_path = subscriber.subscription_path(project, gr_sub)
    snap_path = subscriber.snapshot_path(project, GR_SNAP)

    gr_topics_paths = [grtopic_path, bmtopic_path]

    #sdr_sub = "projects/elene6889/subscriptions/radio-sub"
    tweet_topic = 'projects/{}/topics/twitter-topic'.format(project)
    image_topic = 'projects/{}/topics/traffic-topic'.format(project)
    weather_topic = 'projects/{}/topics/weather-topic'.format(project)
    tweet_sub = 'projects/{}/subscriptions/twitter-sub'.format(project)
    image_sub = 'projects/{}/subscriptions/traffic-sub'.format(project)
    weather_sub = 'projects/{}/subscriptions/weather-sub'.format(project)

    api_topics_paths = [tweet_topic, image_topic, weather_topic]
    api_subs_paths = [tweet_sub, image_sub, weather_sub]

    # Create topics if necessary
    for tpath in gr_topics_paths + api_topics_paths:
        try:
            publisher.create_topic(tpath)
        except AlreadyExists:
            pass
        except Exception as e:
            logging.error('create_topic() threw exception {}'.format(e))
            raise
        logging.info('{} Pub/Sub topic: {}'.format(
            tpath.rsplit('/', 1)[-1].split('-')[0],
            tpath))

    # Create GR subscriptions (with Snapshot logic) independently
    try:
        subscriber.create_subscription(
                grsub_path,
                grtopic_path,
                retain_acked_messages=True)
    except AlreadyExists:
        logging.info('Subscription {} exists!'.format(grsub_path))
        # if subsccription exists, create a snapshot of it
        # NOTE: to retain multiple snapshots and avoid accidentally deleting
        #       an important snapshot, we append a timestamp to the snapshot
        #       path (YYYYMMDDHHMMSS).
        snap_path = snap_path + str(gc_timestamp())
        try:
            subscriber.create_snapshot(snap_path, grsub_path)
            logging.info("Snapshot '{}' for '{}' created!".format(
                snap_path,
                grsub_path))
            #print("Snapshot \'{}\' for \'{}\' created!\n"
            #    .format(snap_path,grsub_path))

        # [RB]: This next block is not needed; will remove after testing
        except Exception as e:
            # RB: if we hit this exception, we should stop. Since we know the
            # subscription exists, this exception is likely due a parent
            # resource/client not found, which is fatal for the pipeline but
            # not immediately so for the program. If we do not exit here,
            # Beam will not not stop attempting to create the Dafaflow job.
            # This process contiues with one retry after another, and the job
            # will eventully need to be killed either from the Dataflow
            # dashboard or using a gcloud command from a terminal.
            #
            # For now, leaving it up to Nick to decide.
            #
            #logging.error(('create_snapshot() threw exception {}').format(e))
            #raise
            logging.warning("Snapshot {} for {} failed!".format(
                snap_path,
                grsub_path))
            logging.error(('create_snapshot() threw exception {}').format(e))
            #print("WARNING: Snapshot \'{}\' for \'{}\' failed!\n"
            #    .format(snap_path,grsub_path))
            pass

        # delete and recreate the subscription to drain it
        try:
            subscriber.delete_subscription(grsub_path)
            logging.info('Subscription {} deleted!'.format(grsub_path))
            subscriber.create_subscription(
                    grsub_path,
                    grtopic_path,
                    retain_acked_messages=True)
            logging.info('Subscription {} created!'.format(grsub_path))
            #subscriber.create_subscription(name=grsub_path,
            #            topic=grtopic_path, retain_acked_messages=True)
            #print("Subscription \'{}\' created!\n".format(grsub_path))
        #except:
        except Exception as e:
            # [RB]: we should exit here. Can't let DataflowRunner get started
            # with a bad subscription
            logging.error('Recreating subscription {} failed!'.format(
                grsub_path))
            logging.error('create_subscription() threw exception {}'.format(e))
            raise
            #print("WARNING: Subscription \'{}\' recreation failed!\n"
            #    .format(grsub_path))
            #pass

    logging.info("GR Pub/Sub subscription: \'{}\'\n".format(grsub_path))

    # Create API subscriptions
    for spath, tpath in zip(api_subs_paths, api_topics_paths):
        try:
            subscriber.create_subscription(spath, tpath)
        except AlreadyExists:
            pass
        except Exception as e:
            logging.error('create_subscription() threw exception {}'.format(e))
            raise
        sname = spath.rsplit('/', 1)[-1].split('-')[0]
        logging.info('{} Pub/Sub subscription: {}'.format(
            spath.rsplit('/', 1)[-1].split('-')[0],
            spath))

# END GOOGLE PUB/SUB CONFIG
###############################################################################

###############################################################################
# GOOGLE BIGQUERY CONFIG
#
    # BigQuery table schema
    from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=import-error,wrong-import-order, wrong-import-position
    table_schema = bigquery.TableSchema()

    # table_schema = 'timestamp:STRING, data:FLOAT'
    # Data field schema
    data_schema = bigquery.TableFieldSchema()
    data_schema.name = 'data'
    data_schema.type = 'float'
    data_schema.mode = 'required'
    table_schema.fields.append(data_schema)
    # Attribute field schema (nested schema)
    attribute_schema =  bigquery.TableFieldSchema()
    attribute_schema.name = 'attribute'
    attribute_schema.type = 'record'
    attribute_schema.mode = 'required'

    timestamp_schema = bigquery.TableFieldSchema()
    timestamp_schema.name = 'localdatetime'
    timestamp_schema.type = 'string'
    timestamp_schema.mode = 'required'
    attribute_schema.fields.append(timestamp_schema)

    centerfreq_schema = bigquery.TableFieldSchema()
    centerfreq_schema.name = 'center_freq'
    centerfreq_schema.type = 'float'
    centerfreq_schema.mode = 'nullable'
    attribute_schema.fields.append(centerfreq_schema)

    samplerate_schema = bigquery.TableFieldSchema()
    samplerate_schema.name = 'sample_rate'
    samplerate_schema.type = 'float'
    samplerate_schema.mode = 'nullable'
    attribute_schema.fields.append(samplerate_schema)

    table_schema.fields.append(attribute_schema)

    # BigQuery table specification
    table_spec = '{project_id}:{dataset_id}.{table_id}'.format(
                  project_id = project,
                  dataset_id = DATASET,
                  table_id = GR_TABLE)
# END BIGQUERY CONFIG
###############################################################################

###############################################################################
# START BEAM PIPELINE
#
    # RB: add options from pipeline_args
    pipeline_options = PipelineOptions(pipeline_args)
    #pipeline_options = PipelineOptions(pipeline_args)
    # use the requirements document to list Python packages required in the
    # pipeline
    # NOTE: this didn't seem to resolve the datetime dependecy I saw
    #pipeline_options.view_as(SetupOptions).requirements_file = 'requirements.txt'
    pipeline_options.view_as(SetupOptions).save_main_session = True # global session to workers
    pipeline_options.view_as(GoogleCloudOptions).job_name = "e6889-project-{}".format(gc_timestamp())
    pipeline_options.view_as(StandardOptions).runner = str(runner)
    if runner == 'DataflowRunner':
      pipeline_options.view_as(StandardOptions).streaming = True
      pipeline_options.view_as(GoogleCloudOptions).project = project
      pipeline_options.view_as(GoogleCloudOptions).temp_location = dflow_temp
      pipeline_options.view_as(GoogleCloudOptions).staging_location = stage

    # Create pipeline and begin Beam processing
    #
    # BEGIN: GNU Radio nodes
    p = beam.Pipeline(options=pipeline_options)
    data = (p | 'Read RF' >>  beam.io.ReadFromPubSub(
                #topic=grtopic_path,
                subscription=grsub_path,
                with_attributes=True,
                timestamp_attribute='timestamp')
              | 'RF Parse Data' >> beam.ParDo(ParseAttrDataFn()))#'RfTuple' >> beam.Map(lambda pubsub:
    # data -> { 'data':X, 'center_freq':X, 'sample_rate':X, 'localdatetime':X }

    # filters input based on threshold value (-threshold,-d)
    signal1 = (data | 'Signal Detector' >> SignalDetector(threshold))
    # debug1 = signal1[0] | 'DebugOutput' >> beam.ParDo(DebugOutputFn())

    # calculates congestion using detected signals (above threshold)
    congestion = (signal1  | 'Congestion Window' >> beam.WindowInto(
                                window.SlidingWindows(60,50)) # overlap by 10 seconds
                           | 'Congestion Group' >> beam.GroupByKey() # group by center_freq, then average
                           | 'Channel Congestion' >> beam.ParDo(ChannelCongestionFn()))
    # congestion -> (center_freq,(channel_cong,data_start,data_end))

    # calculates the average channel PSD (in dBs)
    average = (data | 'Average Create Tuple' >> beam.ParDo(CreateFreqTupleFn()) # (center_freq,(localdatetime,data))
                    | 'Average Window' >> beam.WindowInto(window.SlidingWindows(60,50)) # overlap by 10 seconds
                    | 'Average Group' >> beam.GroupByKey() # group by center_freq, then average
                    | 'Channel Average' >> beam.ParDo(ChannelAmpAvgFn()))
    # average -> (center_freq,(channel_avg,data_start,data_end))

    # group the results
    output = ({'congestion':congestion, 'average':average}
                      | 'GroupAll' >> beam.CoGroupByKey())
    # output -> (center_freq,{'congestion':[], 'average':[]})

    pubsub = (output  | 'Format PubSub' >> beam.ParDo(FormatPubsubOutFn())) # converts above to PubSub Output

    # output to PubSub; requires subscriber
    pubsub | beam.io.WriteToPubSub(topic=bmtopic_path)

    # END: GNU Radio nodes

    # BEGIN: API-sources nodes
    #
    # get stream from traffic camera image source (corresponds to index [5] of medoids)
    traffic_stream = ( p | "Read Image" >> beam.io.ReadFromPubSub(
                                subscription=image_sub, with_attributes=True)
                         | "Traffic Window" >> beam.WindowInto(
                                window.SlidingWindows(45,5),
                                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
                         | 'Traffic Value' >> beam.ParDo(
                                #lambda (c): [(c.attributes['localdatetime'], float(c.attributes['cnn_output']))])
                                lambda (c): [("traffic", float(c.attributes['cnn_output']))])
                         | 'Twitter Mean' >> beam.combiners.Mean.PerKey())
                        #| 'Log Traffic Value' >> beam.ParDo(lambda (c): logging.info("++Traffic_Value++ %s",c)))

    # get stream from twitter source (corresponds to index [0] of medoids)
    twitter_stream = ( p | "Read Twitter" >> beam.io.ReadFromPubSub(
                                subscription=tweet_sub, with_attributes=True)
                         | "Twitter Window" >> beam.WindowInto(
                                window.SlidingWindows(45,5),
                                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
                         #| 'Log Twitter Value' >> beam.ParDo(lambda (c): logging.info("++Twitter_Value++ %s",c.data)))
                         | 'Twitter Value' >> beam.ParDo(lambda (c): [("tweets", 1)])
                         | 'Twitter Count' >> beam.combiners.Count.PerKey())
                         #| 'Log Twitter Value' >> beam.ParDo(lambda (c): logging.info("++Twitter_Value++ %s",c)))

    # get stream from twitter source (corresponds to indices [1],[2],[3],[4] of medoids)
    weather_stream = ( p | "Read Weather" >> beam.io.ReadFromPubSub(
                                subscription=weather_sub, with_attributes=True)
                         | "Weather Window" >> beam.WindowInto(
                                window.SlidingWindows(45,5),
                                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING)
                         #| 'Log Weather Message' >> beam.ParDo(lambda (c): logging.info("++Weather_Value++ %s",c.attributes))
                         | 'Weather Value' >> beam.ParDo(
                                lambda (c): [
                                    ("precip", rain_num(c.attributes['summary'])),
                                    ("temp", float(c.attributes['temp'])),
                                    ("uv", float(c.attributes['uv'])),
                                    ("wind", float(c.attributes['wind']))])
                         | 'Weather Mean' >> beam.combiners.Mean.PerKey())
                        #| 'Log Weather Value' >> beam.ParDo(lambda (c): logging.info("++Weather_Value++ %s",c)))

    merged_stream = ( (traffic_stream, twitter_stream, weather_stream, )
                         | "Join" >> beam.Flatten())

    observed_point = ( merged_stream
                        | "Window" >> beam.WindowInto(
                                window.FixedWindows(60),
                                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
                        | 'Add Key' >> beam.ParDo(lambda (c): [("key", c)])
                        | 'Group' >> beam.GroupByKey()
                        | "Calc Point" >> beam.ParDo(valid_point())
                        | "Normalize Point" >> beam.ParDo(normalize_point())
                        | "Assign Cluster" >> beam.ParDo(assign_cluster())
                        | 'Log Point' >> beam.ParDo(
                                lambda (a,c): logging.info("*** Observed Cluster =  %s",c)))

    # https://stackoverflow.cogrtopic_pathm/questions/51621792/streaming-pipelines-with-bigquery-sinks-in-python
    # Write data to BigQuery
    # payload | beam.io.WriteToBigQuery(
    #   table_spec,
    #   schema=table_schema,
    #   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

     # Execute the Pipline
    result = p.run()
    result.wait_until_finish()

# END PIPELINE
######################################################

######################################################
# CUSTOM PARDO FUNCTIONS
#
# Transform: parses PCollection elements
class ParseAttrDataFn(beam.DoFn):
  """Parses and type converts the raw data and Google Pub/Sub attributes.
    Each Pub/Sub packet has a data payload (representing time varying element
    of the RF spectrum, such as average or maximum power spectrum density at
    the specified center frequency). A Google Pub/Sub packet is expected to be
    in the following format:
    {
      data: 'VALUE' // float
      attributes: {
        "localdatetime":  "YYYY-MM-DD HH:MM:SS"
        "timestamp:"      "YYYY-MM-DDTHH:MM:SS.SSSZ" // RFC 3339 format, UTC
        "center_freq":    "VALUE_IN_HZ" // float
        "sample_rate":    "VALUE_IN_SAMPLE_PER_SEC" // float
      }
    }"""
  # main process
  def process(self,element):
    # logging.debug('ParseAttrDataFn(): Data dump ->  {}\n'.format(element))

    # get data
    try:
      data = element.data
    except Exception as e:
      data = -1.0
      # self.num_parse_errors.inc()
      logging.critical("ParseAttrDataFn(): data type conversion error \'{}\'.".format(e))
    # get local date/time string
    try:
      dt = element.attributes['localdatetime']
    except Exception as e:
      dt = ''
      # self.num_parse_errors.inc()
      logging.critical("ParseAttrDataFn(): datetime parse error  \'{}\'.".format(e))
    # get center frequency of signal for grouping
    try:
      cfreq = element.attributes['center_freq']
    except Exception as e:
      cfreq = None
      logging.warning("ParseAttrDataFn(): center_freq parse error \'{}\'.".format(e))
    # get sample rate (not used as of 2019/04/25)
    try:
      srate = element.attributes['sample_rate']
    except Exception as e:
      srate = None
      logging.warning("ParseAttrDataFn(): sample parse error \'{}\'.".format(e))

    # return a dictionary for subsequent processing.
    yield {
      'data':data,
      'center_freq':cfreq.decode('utf-8'),
      'sample_rate':srate.decode('utf-8'),
      'localdatetime': dt.decode('utf-8')#,
    }

# ParDo Transform: creates tuple with center_freq as key and adds timestamp
class CreateFreqTupleFn(beam.DoFn):
  """ Creates tuple in output format below from dictionary input format below.
      Input Format:   {'data': float, 'center_freq': float, 'sample_rate':float, 'localdatetime': string}\n
      Output Format:  (center_freq,(localdatetime,data))
  """
  def process(self, element):
    # PCollecton format: [key,value,localdt]
    try:
      freq = float(element['center_freq'])
      data = float(element['data'])
      time = element['localdatetime']
    except Exception as e:
      logging.critical("ParseAttrDataFn(): freq/data/time extraction error \'{}\'.".format(e))

    key_value = (freq,(time,data))
    # logging.info("CreateFreqTupleFn(): key_value -> {}".format(key_value))
    yield key_value

# Transform:
class ChannelAmpAvgFn(beam.DoFn):
  """ Takes a PCollection of channel (frequency band) data formatted as shown
      below and returns the channel average PSD in format shown below.

      Input Format:  (center_freq,[(ev_timestamp1,ev1),...,(ev_timestampN,evN)])
      Output Format: (center_freq,(channel_avg,data_start,data_end))
  """
  # main process
  def process(self,element,window=beam.DoFn.WindowParam):
    logging.debug("\n>>>>> ChannelAmpAvgFn(): Data dump -> {}".format(element))

    try:
      events = element[1]
    except Exception as e:
      logging.critical("ChannelAmpAvgFn(): failed to assign element list. ({})"
                        .format(e))

    # grab each of the PSD elements from the list of (timestamp,psd) tuples
    psds = []
    timestamps = []
    for event in events:
      # event = (timestamp,psd)
      psds.append(event[1])
      timestamps.append(event[0])

    logging.debug(">>>>> ChannelAmpAvgFn(): PSDS -> {}".format(psds))

    # calculate channnel average
    if len(psds) != 0:
      channel_avg = sum(psds)/float(len(psds))
      # logging.info("ChannelAmpAvgFn(): channel_avg -> {}".format(channel_avg))
    else:
      channel_avg = 0.0

    # set start/end timestamps provided in data, else the window times
    # try:
    #   start = timestamps[0]
    #   end = timestamps[-1]
    # except:
    start = window.start.to_utc_datetime().strftime(DT_FORMAT)
    end = window.end.to_utc_datetime().strftime(DT_FORMAT)

    # create new (center_freq,(channel_avg,data_start,data_end)) tuple
    key_value = (element[0],(channel_avg,start,end))

    logging.info(">>>>> ChannelAmpAvgFn(): Result -> {}".format(key_value))
    yield key_value

class ChannelCongestionFn(beam.DoFn):
  """ Takes a PCollection of channel (frequency band) data formatted as shown
      below and returns the channel average PSD in format shown below.

      Input Format:  (center_freq,[(ev_timestamp1,ev1),...,(ev_timestampN,evN)])
      Output Format: (center_freq,(channel_avg,data_start,data_end))
  """
  # main process
  def process(self,element,window=beam.DoFn.WindowParam):

    try:
      events = element[1]
    except Exception as e:
      logging.critical("ChannelCongestionFn(): failed to assign element list. ({})"
                        .format(e))

    # grab each of the count elements from the list of (timestamp,1) tuples
    counts = []
    timestamps = []
    for event in events:
      # event = (timestamp,1)
      counts.append(event[1])
      timestamps.append(event[0])

    # set start/end timestamps provided in data, else the window times
    # try:
    #   start = timestamps[0]
    #   end = timestamps[-1]
    # except:
    start = window.start.to_utc_datetime().strftime(DT_FORMAT)
    end = window.end.to_utc_datetime().strftime(DT_FORMAT)

    # create new (center_freq,(channel_avg,data_start,data_end)) tuple
    key_value = (element[0],(sum(counts),start,end))

    logging.info(">>>>> ChannelCongestionFn(): Result -> {}".format(key_value))
    yield key_value

class FormatPubsubOutFn(beam.DoFn):
  """ Formats output for Google PubSub
      Input Format:
      (center_freq, {'congestion': [(channel_cong,start_tim,end_time)],\n
        'average': [(channel_avg,start_time,end_time)]})\n

      Output Format:  [center_freq: {'congestion': [(channel_cong,start_tim,end_time)],\n
        'average': [(channel_avg,start_time,end_time)]}]

  """
  # main process
  def process(self,element):#,window=beam.DoFn.WindowParam):
    # logging.debug(">>>>> FormatPubsubOutFn(): Element -> {}".format(element))
    jsonstr = "{{\'{cfreq}\': {data}}}".format(cfreq = element[0],data=element[1])
    jsonstr = jsonstr.replace("'", "\"")
    logging.info(">>>>> FormatPubsubOutFn(): Dictionary -> {}".format(jsonstr))

    return [jsonstr]

    # Transform:
class DebugOutputFn(beam.DoFn):
  """ Formats output

  """
  # main process
  def process(self,element):#,window=beam.DoFn.WindowParam):
    # logging.info(">>>>> DebugOutputFn(): Result -> {}".format(element))
    return None
# END PARDOS
######################################################


######################################################
# CUSTOM PTRANSFORMS
#
# # PTransform: Filters noise from input based on user defined threshold
# established at runtime (-threshold, -d) and provides signals of interest
class SignalDetector(beam.PTransform):
  """A transform to filter the non-peak (noise) out of the input signal. This
  transform assumes the input format below and provides the output format shown
  below.

  Input Format:   {'data': float, 'center_freq': str, 'sample_rate':str, 'localdatetime': str}\n
  Output Format:  (center_freq,(localdatetime,data'))  where data' is data > threshold
  """
  def __init__(self, threshold):
    super(SignalDetector, self).__init__()
    self.threshold = threshold

  def expand(self, pcoll):
    def peakMarker(element):
      element['data'] = 1
      return element

    # logging.info('SignalDetector(): averaging {} \n'.format(self.target))
    signal = (pcoll | 'PeakFilter' >> beam.Filter(lambda parsed:
                                        float(parsed['data']) >= self.threshold))  # filter noise from data

    # return tuple with frequency PSD values as data
    freq = signal | 'CreateTupleFreq' >> beam.ParDo(CreateFreqTupleFn())  # -> (center_freq,(localdatetime,data))
    debug1 = freq | 'DebugOutput' >> beam.ParDo(DebugOutputFn())

    # return tuple with event count values as data
    count = (signal | 'PeakMarker' >> beam.Map(peakMarker)#beam.Map(lambda peak: (peak[0],1))
                    | 'CreateTupleCount' >> beam.ParDo(CreateFreqTupleFn()))

    return count


# END CUSTOM PTRANSFORMS
######################################################
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

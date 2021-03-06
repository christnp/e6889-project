#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Columbia University, 2019
# ELEN-E6889: Homework 2
#
# Author:   Nick Christman, nc2677
# Date:     2019/04/20
# Program:  gnu-radio-pipeline.py
# SDK(s):   Apache Beam/Google Dataflow, Google Pub/Sub, Google BigQuery

'''This file implements a Beam Pipeline for processing data from GNU Radio. The
data source comes from a GNU Radio application that publishes the data periodi-
cally to the cloud using the Google Pub/Sub framework.

Assumptions:
  1) Ingested data is in the following format
        {
            data: 'VALUE'
            attributes: {
                "localdatetime":  "YYYY-MM-DD HH:MM:SS"
                "center_freq":    "VALUE_IN_HZ"
                "sample_rate":    "VALUE_IN_SAMPLE_PER_SEC"
            }
        }
  2) A Write transform to a BigQuerySink accepts PCollections of dictionaries.

 References:
 1.  https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
'''

import argparse
import datetime
import pytz
import logging
import sys
import os
import time
import json

import apache_beam as beam # pylint: disable=import-error
from apache_beam import window # pylint: disable=import-error
from apache_beam.pvalue import AsIter # pyli  nt: disable=import-error
from apache_beam.metrics.metric import Metrics # pylint: disable=import-error
from apache_beam.transforms.core import Windowing # pylint: disable=import-error
# from apache_beam.transforms.window import FixedWindows # pylint: disable=import-error
# from apache_beam.transforms.window import SlidingWindows # pylint: disable=import-error
from apache_beam.transforms.trigger import AfterProcessingTime # pylint: disable=import-error
from apache_beam.transforms.trigger import AccumulationMode # pylint: disable=import-error
from apache_beam.transforms.trigger import AfterWatermark # pylint: disable=import-error
from apache_beam.options.pipeline_options import PipelineOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import SetupOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import StandardOptions # pylint: disable=import-error
from apache_beam.options.pipeline_options import GoogleCloudOptions # pylint: disable=import-error

from google.cloud import pubsub_v1 # pylint: disable=import-error,no-name-in-module
# from google.cloud import bigquery

# primary constants
DT_FORMAT = '%Y-%m-%d %H:%M:%S'
GC_DT_FORMAT = '%Y%m%d-%H%M%S'
PROJECT = 'elen-e6889'
GR_TOPIC = 'gnuradio'
GR_SUB = 'gnuradio-sub'
GR_SNAP = 'gnuradio-snap'
GR_TABLE = 'rf_samples'
DATASET = 'project_data'  

# other constants
BEAM_TOPIC = 'beam-top'
DFLOW_TEMP = 'gs://e6889-bucket/tmp/'
STAGE = 'gs://e6889-bucket/stage/'
RUNNER = 'DataflowRunner'
THRESHOLD = 1.0

######################################################
# HELPER FUNCTIONS
#
# to calculate timestamp for google cloud applications (different than display format)
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
######################################################

######################################################
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

######################################################
# GOOGLE PUB/SUB CONFIG
#
    subscriber = pubsub_v1.SubscriberClient()
    grtopic_path = subscriber.topic_path(project, gr_topic)
    sub_path = subscriber.subscription_path(project,gr_sub)
    snap_path = subscriber.snapshot_path(project,GR_SNAP)

    bmtopic_path = subscriber.topic_path(project, bm_topic)

    # RB: ensuring the topic exists. Please see my notes on the exceptions
    # raised by create_subscription()
    #
    from google.api_core.exceptions import AlreadyExists, NotFound
    publisher = pubsub_v1.PublisherClient()
    try:
        publisher.create_topic(grtopic_path)
        logging.info('New topic {} created!'.format(grtopic_path))
    except AlreadyExists:
        pass
    except Exception as e:
        logging.error('create_topic() threw exception {}'.format(e))
        raise

    # create subscription; if it exists create snapshot then drain
    #
    try:
        subscriber.create_subscription(
            name=sub_path, topic=grtopic_path,
            retain_acked_messages=True)
    #except:
    except NotFound as e:
        logging.error(('Topic not found: create_subscription() threw '
                'exception: {}').format(e))
        raise

    except AlreadyExists:
        logging.info('Subscription {} exists!'.format(sub_path))
        # NOTE: to retain multiple snapshots and avoid accidentally deleting
        #       an important snapshot, we append a timestamp to the snapshot
        #       path (YYYYMMDDHHMMSS).
        snap_path = snap_path + str(gc_timestamp())
        try:
            subscriber.create_snapshot(snap_path,sub_path)
            print("Snapshot \'{}\' for \'{}\' created!\n"
                .format(snap_path,sub_path))
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
            # For now, leaving it up to Nick to decide. NC: creating a snapshot
            # is not critical for the functionality of the pipeline, so I will
            # leave it as a warning message, the subscription will just 
            # be drained.
            #
            #logging.error(('create_snapshot() threw exception {}').format(e))
            #raise
            print("WARNING: Snapshot \'{}\' for \'{}\' failed!\n"
                .format(snap_path,sub_path))
            pass

        # delete and recreate the subscription to drain it
        try:
            subscriber.delete_subscription(sub_path)
            print("Subscription \'{}\' deleted!".format(sub_path))
            subscriber.create_subscription( name=sub_path,
                        topic=grtopic_path, retain_acked_messages=True)
            print("Subscription \'{}\' created!\n".format(sub_path))
        except:
            print("WARNING: Subscription \'{}\' recreation failed!\n"
                .format(sub_path))
            pass

    logging.info("Google Pub/Sub topic: \'{}\'".format(grtopic_path))
    logging.info("Google Pub/Sub subscription: \'{}\'\n".format(sub_path))
# END GOOGLE PUB/SUB CONFIG
######################################################

######################################################
# GOOGLE BIGQUERY CONFIG
# NOTE: This is currently not being used as an output IO, but it was left for future use

    # BigQuery table schema
    # TODO: need to install apache_beam[gcp] for his to work (FIY: errors to not indicate this dependency)
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
######################################################

######################################################
# START BEAM PIPELINE
#
    pipeline_options = PipelineOptions(pipeline_args)
    # use the requirements document to list Python packages required in the pipeline
    #pipeline_options.view_as(SetupOptions).requirements_file = 'requirements.txt'
    pipeline_options.view_as(SetupOptions).save_main_session = True # global session to workers
    pipeline_options.view_as(GoogleCloudOptions).job_name = "e6889-project-{}".format(gc_timestamp())
    pipeline_options.view_as(StandardOptions).runner = str(runner)
    if runner == 'DataflowRunner':
      pipeline_options.view_as(StandardOptions).streaming = True
      pipeline_options.view_as(GoogleCloudOptions).project = project
      pipeline_options.view_as(GoogleCloudOptions).temp_location = dflow_temp
      pipeline_options.view_as(GoogleCloudOptions).staging_location = stage

    # Define pipline
    p = beam.Pipeline(options=pipeline_options)

    # Begin pipeline
    output = p | 'GNU Radio Node' >> GnuRadioPipeline(threshold=threshold,subscription=sub_path)
    # NC - 20190516: moved pipeline to custom PTransform for modularity

    # TODO: merge output with clustering pipeline

    # output to PubSub; requires subscriber
    pubsub = (output  | 'Format PubSub' >> beam.ParDo(FormatPubsubOutFn())) # converts above to PubSub Output
    pubsub | beam.io.WriteToPubSub(topic=bmtopic_path)

    # https://stackoverflow.cogrtopic_pathm/questions/51621792/streaming-pipelines-with-bigquery-sinks-in-python
    # Write data to BigQuery
    # NOTE: this is currently not used but is left for future use
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
    logging.debug("\ngrpl>> ChannelAmpAvgFn(): Data dump -> {}".format(element))

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

    logging.debug("grpl>> ChannelAmpAvgFn(): PSDS -> {}".format(psds))

    # calculate channnel average
    if len(psds) != 0:
      channel_avg = sum(psds)/float(len(psds))
      # logging.info("ChannelAmpAvgFn(): channel_avg -> {}".format(channel_avg))
    else:
      channel_avg = 0.0

    start = window.start.to_utc_datetime().strftime(DT_FORMAT)
    end = window.end.to_utc_datetime().strftime(DT_FORMAT)

    # create new (center_freq,(channel_avg,data_start,data_end)) tuple
    key_value = (element[0],(channel_avg,start,end))

    logging.info("grpl>> ChannelAmpAvgFn(): Result -> {}".format(key_value))
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

    start = window.start.to_utc_datetime().strftime(DT_FORMAT)
    end = window.end.to_utc_datetime().strftime(DT_FORMAT)

    # create new (center_freq,(channel_avg,data_start,data_end)) tuple
    key_value = (element[0],(sum(counts),start,end))

    logging.info("grpl>> ChannelCongestionFn(): Result -> {}".format(key_value))
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
    # logging.debug("grpl>> FormatPubsubOutFn(): Element -> {}".format(element))
    jsonstr = "{{\'{cfreq}\': {data}}}".format(cfreq = element[0],data=element[1])
    jsonstr = jsonstr.replace("'", "\"")
    logging.info("grpl>> FormatPubsubOutFn(): Dictionary -> {}".format(jsonstr))

    return [jsonstr]

    # Transform:
class DebugOutputFn(beam.DoFn):
  """ Formats output

  """
  # main process
  def process(self,element):#,window=beam.DoFn.WindowParam):
    logging.debug("grpl>> DebugOutputFn(): Result -> {}".format(element))
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
    freq = signal | 'CreateTupleFreq' >> beam.ParDo(CreateFreqTupleFn())  # (center_freq,(localdatetime,data))
    
    # Used for debugging
    debug1 = freq | 'DebugOutput' >> beam.ParDo(DebugOutputFn())

    # return tuple with event count values as data
    count = (signal | 'PeakMarker' >> beam.Map(peakMarker)#beam.Map(lambda peak: (peak[0],1))
                    | 'CreateTupleCount' >> beam.ParDo(CreateFreqTupleFn()))

    return count

class GnuRadioPipeline(beam.PTransform):

  def __init__(self, threshold, subscription):
    super(GnuRadioPipeline,self).__init__()
    self.threshold = threshold
    self.sub_path = subscription
  
  def expand(self,pcoll):
    data = (pcoll | 'GetData' >>  beam.io.ReadFromPubSub(
                                  #   topic=grtopic_path,
                                  subscription=self.sub_path,
                                  with_attributes=True,
                                  timestamp_attribute='timestamp')
              | 'ParseData' >> beam.ParDo(ParseAttrDataFn()))#'RfTuple' >> beam.Map(lambda pubsub:
    # Pcollection: data -> { 'data':X, 'center_freq':X, 'sample_rate':X, 'localdatetime':X }

    # filters input based on threshold value (-threshold,-d)
    # signal1 = (data | 'SignalDetector' >> SignalDetector(threshold))
   

    # calculates congestion using detected signals (above threshold)
    congestion = (data  | 'SignalDetector' >> SignalDetector(self.threshold) # filters input based on threshold value (-threshold,-d)
                        | 'WindowCongestion' >> beam.WindowInto(window.SlidingWindows(60,50)) # overlap by 10 seconds
                        | 'GroupCongestion' >> beam.GroupByKey() # group by center_freq, then count peaks
                        | 'ChannelCongestion' >> beam.ParDo(ChannelCongestionFn()))
    # Pcollection: congestion -> (center_freq,(channel_cong,data_start,data_end))

    # calculates the average channel PSD (in dBs)
    average = (data | 'CreateTupleAverage' >> beam.ParDo(CreateFreqTupleFn()) # (center_freq,(localdatetime,data))
                    | 'WindowAverage' >> beam.WindowInto(window.SlidingWindows(60,50)) # overlap by 10 seconds
                    | 'GroupAverage' >> beam.GroupByKey() # group by center_freq, then average
                    | 'ChannelAverage' >> beam.ParDo(ChannelAmpAvgFn()))
    # Pcollection: average -> (center_freq,(channel_avg,data_start,data_end))

    # group the results
    output = ({'congestion':congestion, 'average':average}
                      | 'GroupAll' >> beam.CoGroupByKey())
    # Pcollection: output -> (center_freq,{'congestion':[], 'average':[]})

    return output

# END CUSTOM PTRANSFORMS
######################################################
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

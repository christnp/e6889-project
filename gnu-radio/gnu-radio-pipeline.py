#!/usr/bin/env python
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

import apache_beam as beam # pylint: disable=import-error
from apache_beam import window # pylint: disable=import-error
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
OUT_TOPIC = 'output'
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

# END HELPER FUNCTIONS
######################################################

######################################################
# MAIN PROCESSING FUNCTION
#
def run(argv=None):
    parser = argparse.ArgumentParser()
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
                        dest='out_topic',
                        default=OUT_TOPIC,
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
    args = parser.parse_args()
    project = args.project
    gr_topic = args.gr_topic
    gr_sub = args.gr_sub
    out_topic = args.out_topic
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
    topic_path = subscriber.topic_path(project, gr_topic)
    sub_path = subscriber.subscription_path(project,gr_sub)
    snap_path = subscriber.snapshot_path(project,GR_SNAP)

    # create subscription; if it exists create snapshot then drain 
    try:
        subscriber.create_subscription(
            name=sub_path, topic=topic_path,
            retain_acked_messages=True)
    except:
        print("Subscription \'{}\' exists!\n".format(sub_path))
        # if subsccription exists, create a snapshot of it
        # NOTE: to retain multiple snapshots and avoid accidentally deleting
        #       an important snapshot, we append a timestamp to the snapshot
        #       path (YYYYMMDDHHMMSS).
        snap_path = snap_path + str(gc_timestamp())
        try:
            subscriber.create_snapshot(snap_path,sub_path)
            print("Snapshot \'{}\' for \'{}\' created!\n"
                .format(snap_path,sub_path))
        except:
            print("WARNING: Snapshot \'{}\' for \'{}\' failed!\n"
                .format(snap_path,sub_path))
            pass

        # delete and recreate the subscription to drain it
        try:
            subscriber.delete_subscription(sub_path)
            print("Subscription \'{}\' deleted!".format(sub_path))
            subscriber.create_subscription( name=sub_path, 
                        topic=topic_path, retain_acked_messages=True)
            print("Subscription \'{}\' created!\n".format(sub_path))
        except:
            print("WARNING: Subscription \'{}\' recreation failed!\n"
                .format(sub_path))
            pass

    logging.info("Google Pub/Sub topic: \'{}\'".format(topic_path))
    logging.info("Google Pub/Sub subscription: \'{}\'\n".format(sub_path))
# END GOOGLE PUB/SUB CONFIG
######################################################

######################################################
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
######################################################

######################################################
# START BEAM PIPELINE
#
    pipeline_options = PipelineOptions()
    # use the requirements document to list Python packages required in the pipeline
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

    # Define pipline
    p = beam.Pipeline(options=pipeline_options)
    data = (p | 'GetData' >>  beam.io.ReadFromPubSub(  
                                  #   topic=topic_path,
                                  subscription=sub_path,
                                  with_attributes=True,
                                  timestamp_attribute='timestamp'))

    peaks = (data | 'ParseData' >> beam.ParDo(ParseAttrDataFn())#'RfTuple' >> beam.Map(lambda pubsub: 
                                # (float(pubsub.attributes['center_freq']),
                                # float(pubsub.data)))
                    | 'PeakFilter' >> beam.Filter(lambda parsed:  
                                        float(parsed['data']) >= threshold) # filter noise
                    | 'CreateTuple' >> beam.ParDo(CreateFreqTupleFn()))
                    # | 'CreateRFTuple' >> beam.Map(lambda parsed: 
                    #                     (float(parsed['center_freq']),
                    #                     (parsed['localdatetime'],                                        
                    #                     float(parsed['data'])))))                    
                    #| 'PeakMarker' >> beam.Map(lambda peak: (peak[0],1)))

    signal1 = (peaks  | 'Window' >> beam.WindowInto(window.SlidingWindows(60,59))
                      # | 'SignalDet1' >> beam.ParDo(SignalDetector1Fn(threshold)))
                      | 'Group' >> beam.GroupByKey())

    output = signal1 | 'FormatOutput' >> beam.ParDo(FormatOutputFn()
    )
    # signal2 = (payload  | 'Window' >> beam.WindowInto(window.SlidingWindows(60,59))
                        # |'SignalDet2' >> beam.ParDo(SignalDetector2Fn()))

    # https://stackoverflow.cotopic_pathm/questions/51621792/streaming-pipelines-with-bigquery-sinks-in-python
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
    }

  """
  def __init__(self): 
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  # main process
  def process(self,element):
    logging.debug('ParseAttrDataFn(): Data dump ->  {}\n'.format(element)) 

    # get data
    try:
      data = element.data
    except Exception as e:
      data = -1.0
      self.num_parse_errors.inc()
      logging.critical("ParseAttrDataFn(): data type conversion error \'{}\'.".format(e))
    # get timestamp for windowing based on collection time
    # try:
    #   timestamp = element.attributes['timestamp']
    # except Exception as e:
    #   timestamp = 0
    #   self.num_parse_errors.inc()
    #   logging.critical("ParseAttrDataFn(): timestamp parse error \'{}\'.".format(e))
    # get local date/time string
    try:
      dt = element.attributes['localdatetime']
    except Exception as e:
      dt = ''
      self.num_parse_errors.inc()
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
      # 'timestamp': timestamp.decode('utf-8')
    }
    # yield element

# ParDo Transform: creates tuple with center_freq as key and adds timestamp
class CreateFreqTupleFn(beam.DoFn):
  def process(self, element):
    # PCollecton format: [key,value,google_ts]
    try:
      freq = float(element['center_freq'])
      data = float(element['data'])
      time = element['localdatetime']
    except Exception as e:
      logging.critical("ParseAttrDataFn(): freq/data/time extraction error \'{}\'.".format(e))
        
    key_value = (freq,(time,data))
    logging.info("CreateFreqTupleFn(): key_value -> {}".format(key_value))
    # yield window.TimestampedValue(key_value,google_ts)
    yield key_value

# Transform:   
class SignalDetector1Fn(beam.DoFn):
  """ Detects signal on streaming data given a threshold

  """
  def __init__(self,threshold=0): 
    self.threshold = threshold
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  # main process
  def process(self,element):
    logging.debug("\nSignalDetector1Fn(): Data dump -> {}".format(element))
    logging.debug("\nSignalDetector1Fn(): Threshold -> {}".format(self.threshold))
    
    # local variables
    try:
      psd_float = float(element[1])
    except Exception as e:
      logging.info("SignalDetector1Fn(): failed to cast data to float \
                        (error: {0}, data: {1})".format(e, element[1]))
     
    # if psd_float >= self.threshold:
    # logging.info("Signal @ {0}? ...... {1}".format(element[0],psd_float))
    # logging.info("Threshold .......... {}".format(self.threshold))
    
    if psd_float >= self.threshold:
      yield element
    else:
      return  # Return nothing

    
    # Transform:   
class FormatOutputFn(beam.DoFn):
  """ Formats output

  """
  def __init__(self,): 
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  # main process
  def process(self,element,window=beam.DoFn.WindowParam):

    win_start = window.start.to_utc_datetime().strftime(DT_FORMAT)
    win_end = window.end.to_utc_datetime().strftime(DT_FORMAT)

    logging.info("output element ....... {}".format(element))
    logging.info("window start ......... {}".format(win_start))
    logging.info("window end ........... {}\n".format(win_end))
    # END PARDOS
  ######################################################

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Rashad Barghouti (UNI:rb3074)
# Columbia University, New York
# ELEN E6889, Spring 2019
#
#——————————————————————————————————————————————————————————————————————————————

# Standard library imports
from __future__ import absolute_import, print_function
import os
import sys
import time
import logging
import argparse
import pendulum
from pprint import pformat

# GC and Beam imports
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import ReadFromPubSub
from apache_beam.pvalue import AsDict
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

# Project imports
import twacct
import e6889_project as ep

ArgumentValueError = ep.ArgumentValueError

# Program argument defaults are DirectflowRunner values
args_defaults = {'project': os.getenv('E6889_PROJECT_ID'),
                 'input_topic_name': 'twitter-stream',
                 'input_sub_name': 'twitter-stream',
                 'output_topic_name': 'pipeline-output',
                 'output_sub_name': 'pipeline-output',
                 'runner': None,
                 'streaming': True,
                 'temp_location': None,
                 'staging_location': None,
                 'job_name': os.path.basename(os.path.splitext(__file__)[0])}

futures = []

# Geo bounding boxes used in requests to Twitter API (see README)
BOUNDING_BOX = [-73.961967, 40.810504, -73.952234, 40.818940]
BOUNDING_BOX_EXTENDED = [-73.964927, 40.807030, -73.952234, 40.818940]

TZ_PACIFIC = 'America/Los_Angeles'
TZ_EASTERN = 'America/New_York'
#STRFTIME_DTSTAMP= '%Y-%m-%d %H:%M:%S %Z%z'
DTSTAMP = 'DD-MMM-YYYY HH:mm:ss.SSS zzZZ'
FILETAG_DTSTAMP = 'MMDD-HHmm'

def dtstamp(fmt=DTSTAMP, timezone=None):
    """Return datetime stamp."""
    dt = pendulum.now(tz=timezone).format(fmt)
    return '{}'.format(dt)
    #return '{}'.format(pendulum.now(tz=timezone).to_cookie_string())
    #return '{}'.format(pendulum.now(tz=timezone).strftime(STRFTIME_TSTAMP))

class ProgramOptions(PipelineOptions):
    """Parser for project-specific arguments."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
                '--input_topic_name',
                nargs='?',
                default=args_defaults['input_topic_name'],
                # const handles case of option but no arg
                const=args_defaults['input_topic_name'],
                help=argparse.SUPPRESS)
        parser.add_argument(
                '--input_sub_name',
                nargs='?',
                default=args_defaults['input_sub_name'],
                # const handles case of option but no arg
                const=args_defaults['input_sub_name'],
                help=argparse.SUPPRESS)
        parser.add_argument(
                '--output_topic_name',
                nargs='?',
                default=args_defaults['output_topic_name'],
                # const handles case of option but no arg
                const=args_defaults['output_topic_name'],
                help=argparse.SUPPRESS)
        parser.add_argument(
                '--output_sub_name',
                nargs='?',
                default=args_defaults['output_sub_name'],
                # const handles case of option but no arg
                const=args_defaults['output_sub_name'],
                help=argparse.SUPPRESS)

def init_runtime():
    """Parse the command line and init Twitter API access and pipeline options.

    Arguments:
        None

    Returns:
        tw_acct          — namedtuple of Twitter account credentials
        pipeline_options — an instance of PipelineOptions()
    """

    # Don't go any further if any of the Twitter credentials has not be set
    if not all(twacct.data.values()):
        logging.error('invalid Twitter API key(s)')
        raise ArgumentValueError('invalid value(s) in twacct.data')
    tw_acct = ep.TwitterAccount(**twacct.data)

    parser = argparse.ArgumentParser(
        usage='%(prog)s [-h] [@argsfile] PIPELINE-ARGS\n\n',
        description=ep.cmdline_helpstr.format(**args_defaults),
        fromfile_prefix_chars='@',
        add_help=False,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--help', '-h', action='help', help=argparse.SUPPRESS)

    # All cmdline args are pipeline args; there are no known_args
    _, plargs = parser.parse_known_args()
    logging.info('{}'.format(dtstamp()))

    pipeline_options = PipelineOptions(plargs)
    program_options = pipeline_options.view_as(ProgramOptions)
    standard_options = pipeline_options.view_as(StandardOptions)
    gcloud_options = pipeline_options.view_as(GoogleCloudOptions)

    # project-id cannot be missing
    project = gcloud_options.project or args_defaults['project']
    if project is None:
        logging.error('GC project ID not found; use --project to set it')
        raise ArgumentValueError(project)
    gcloud_options.project = project

    pipeline_options.view_as(SetupOptions).save_main_session = True
    standard_options.streaming = True

    # Beam's StandardOptions() parser doesn't set a default runner
    runner = standard_options.runner
    if runner is None or runner not in ['DirectRunner', 'DataflowRunner']:
        if runner is None:
            logging.error('--runner argument missing')
        else:
            logging.error(('invalid --runner argument {}. Choose from '
                '(DirectRunner, DataflowRunner)').format(runner))
        sys.exit(0)

    if runner == 'DataflowRunner':
        if gcloud_options.temp_location is None:
            logging.error('--temp_location argument missing. Required when '
                    'pipeline runner is the DataflowRunner.')
            raise ArgumentValueError(gcloud_options.temp_location)
        if gcloud_options.job_name is None:
            gcloud_options.job_name = args_defaults['job_name']
        # allowed chars in job_name: [-a-z0-9]
        gcloud_options.job_name += '-' + dtstamp(FILETAG_TSTAMP)

    logging.info('project:{}, input_topic_name:{}, input_sub_name:{}'.format(
        gcloud_options.project,
        program_options.input_topic_name,
        program_options.input_sub_name))

    logging.info(('runner:{}, streaming:{}, job_name:{}, '
        'save_main_session:{}').format(
            standard_options.runner,
            standard_options.streaming,
            gcloud_options.job_name,
            pipeline_options.view_as(SetupOptions).save_main_session))
    logging.info('temp_location:{}'.format(gcloud_options.temp_location))
    logging.info('staging_location:{}'.format(gcloud_options.staging_location))

    return tw_acct, pipeline_options

def shutdown(twpbsb=None, futures=None):
    """Disconnect from Twitter endpoint and shutdown any subscriber threads."""
    if twpbsb:
        twpbsb.Stream.disconnect()
    if futures:
        for future in futures:
            future.cancel()
            while not future.cancelled():
                pass
            else:
                logging.info('Subscription thread has been shutdown')
    logging.info('shutdown complete')
    sys.exit(0)

    # Need to do quite a bit more here, e.g.,
    #   - shutdown Dataflow jobs, each of which creates a VM
    #   - delete storage buckets
    #   - delete topics and subscriptions
    #   - shutdown arch-vm

#——————————————————————————————————————————————————————————————————————————————
class DisplaySubMsgFn(beam.DoFn):

    def process(self, msg, timestamp=beam.DoFn.TimestampParam):
        """Display the content of the Pub/Sub message as received."""
        if msg.data:
            text = msg.data.decode('utf-8')
            #text = u'{}'.format(msg.data.decode('utf-8'))
        if msg.attributes:
            attrs_str = pformat(msg.attributes)
            attrs_list = msg.attributes.items()
        logging.info('Pipeline input message: Beam timestamp: %d', timestamp)
        #logging.info(' Message attributes: {}'.format(attrs_str))
        logging.info(' Message attributes: %s', attrs_str)
        logging.info(' Message text: %s', text)
        yield timestamp, attrs_list, text

class CreatePubMsgFn(beam.DoFn):

    def process(self, msg):
        data = msg[2].encode('utf-8')
        attrs = dict(msg[1])
        attrs_str = pformat(attrs)
        logging.info('Pipeline output pub msg: data: %s', data)
        logging.info(' Message attributes: %s', attrs_str)
        #yield data, attrs
        yield data

#class UnpackSubMsgFn(bean.DoFn):
#
#    def process(self, msg, timestamp=beam.DoFn.TimestampParam):
#
#        d, a = msg.data, msg.attributes
#        if d:
#            text = d.decode('utf-8')
#        # Keep string attributes utf-8-encoded (don't use format() or str())
#        if a:
#            tweet_id = a.id_str
#            author_id a.author_id
#        yield a.id_str, a.author_id
#

#class ComputeTweetingRateFn(beam.DoFn):
#    """Keeps a running average of the number of Tweets."""
#
#    def process(self, element, tm, perflist):
#        s = '{} {}'.format(element, round(tm, 2))
#        perflist.append(s)
#        return [s]

def directrunner_pipeline(twpbsb, pipeline_options):
    """Launch a pipeline to process geo-tagged Tweets using DirectRunner."""
    logging.info('Starting')

    with beam.Pipeline(options=pipeline_options) as p:

        msgs = (p | 'sub-msgs' >> ReadFromPubSub(
                            subscription=twpbsb.input_sub,
                            with_attributes=True,
                            timestamp_attribute='timestamp_ms'))
        ## Dump received message content
        msgtups = msgs | 'get-tups' >> beam.ParDo(DisplaySubMsgFn())

        # frequency over 24-hour
        # Top hashtags
        # Source devices

        # pipe to output topic
        pubmsgs = (msgtups
                | 'create-msg' >> beam.ParDo(CreatePubMsgFn())
                | 'publish-msg' >> beam.io.WriteToPubSub(twpbsb.output_topic))


def dataflowrunner_pipeline(twpbsb, pipeline_options):
    """Launch a pipeline to process geo-tagged Tweets using DataflowRunner."""
    logging.info('Starting')
    shutdown(twpbsb)

    with beam.Pipeline(options=pipeline_options) as p:

        messages = (p | 'sub-messages' >> ReadFromPubSub(
                            subscription=twpbsb.input_sub,
                            id_label='id_str',
                            with_attributes=True,
                            timestamp_attribute='timestamp_ms'))

        tuples = (messages
                    | 'unpack' >> beam.ParDo(UnpackMessageFn())
                    | 'windowfixed' >> beam.WindowInto(
                        window.FixedWindows(5, 0)))

        disp = tuples | beam.ParDo(PrintTupleFn())


def _main():

    tw_acct, pipeline_options = init_runtime()

    # Create Pub/Sub Clients and Tweepy instances
    twpbsb = ep.TwitterStreamPubSub(tw_acct,
            pipeline_options.view_as(GoogleCloudOptions).project,
            pipeline_options.view_as(ProgramOptions))
    logging.info('Pub/Sub input topic: {}'.format(twpbsb.input_topic))
    logging.info('Pub/Sub input subscription: {}'.format(twpbsb.input_sub))

    # Launch Twitter stream
    twpbsb.Stream.filter(locations=BOUNDING_BOX_EXTENDED, is_async=True)

    try:
        if pipeline_options.view_as(StandardOptions).runner == 'DirectRunner':
            directrunner_pipeline(twpbsb, pipeline_options)
        else:
            dataflowrunner_pipeline(twpbsb, pipeline_options)
    except KeyboardInterrupt:
        logging.info('\nGot ctrl-c. Shutting down')
        shutdown(twpbsb)

    #future = twpbsb.subscribe()
    #futures.append(future)
    #print('Listening for messages on {}'.format(subscription))
    #while True:
    #    try:
    #        time.sleep(15)
    #    except KeyboardInterrupt:
    #        print('\nGot ctrl-c. Shutting down.')
    #        shutdown(twpbsb, future)
    #
    #try:
    #    twpbsb.subscribe().result()
    #except Exception as e:
    #    print('gcbeam: future.result() threw exception: {}.'.format(e))
    #    raise


#——————————————————————————————————————————————————————————————————————————————
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
            format='%(levelname)s:%(module)s:%(funcName)s: %(message)s')
    _main()


#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Project
# 
# Author:   Nick Christman, nc2677
# Date:     2019/04/02
# Program:  gnu_radio_subscriber.py
# SDK(s):   Google Cloud Pub/Sub API


'''This file is designed to subscibe to an existing Google Pub/Sub topic. A
subscription is created. Results are saved to a CSV file for offline analysis.

 References:
 1. https://codelabs.developers.google.com/codelabs/cpb104-pubsub/
'''

# Begin imports
import argparse
import logging
import sys
import os
import re
from datetime import datetime
import csv
import json
#import datetime
import time
import gzip
from google.cloud import pubsub

# constants
DT_FORMAT = '%Y-%m-%d %H:%M:%S'
GC_DT_FORMAT = '%Y%m%d-%H%M%S'
PROJECT = 'elen-e6889'
TOPIC_IN = 'beam-top'
SUBSCRIPTION = 'beam-sub'
CSV_OUTPUT = 'beam_output_{}.csv'.format(datetime.now())
ALDO_CRED = '/home/christnp/Development/e6889/Google/Aldo_key/ELENE6889-7a8999b5aa9f.json'
TEST_CRED = '/home/christnp/Development/e6889/Google/sandbox/elene6889-sandbox-subscriber.json'

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-p',
                        dest='project',
                        default=PROJECT,
                        help='Google project used for Pub/Sub and Dataflow. ' \
                          'This project mus be configured prior to executing ' \
                          'the pipeline. Default: \'elen-e6889\'')
    parser.add_argument('--input', '-i',
                        dest='topic_in',
                        default=TOPIC_IN,
                        help='Input topic for subscribing. The input topic ' \
                          'must be configured prior to executing the ' \
                          'pipeline. If using the included simulator, the '\
                          'topic will be created if it does not exist. '\
                          'Default: \'util-sub\'')
    parser.add_argument('--output', '-o',
                        dest='output',
                        default=CSV_OUTPUT,
                        help='Path of output/results file. If only name is ' \
                          'provided then local directory selected.')
    parser.add_argument('--subscription', '-s',
                        dest='subscription',
                        default=SUBSCRIPTION,
                        help='Subscription name. The input topic ' \
                          'must be configured prior to executing the ' \
                          'pipeline. If using the included simulator, the '\
                          'topic will be created if it does not exist. '\
                          'Default: \'util-simulator\'')                   
                        

    args = parser.parse_args()
    out_path = args.output
    project = args.project
    topic_in = args.topic_in
    subscription = args.subscription

    # set the Google Credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = TEST_CRED #ALDO_CRED

    subscriber = pubsub.SubscriberClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project,
        topic=topic_in,
    )
    subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
        project_id=project,
        sub=subscription,
    )

    try:
        subscriber.create_subscription(
            name=subscription_name, topic=topic_name)
    except:
        print("Subscription \'{}\' already exists!\n".format(subscription_name))

     # initialize CSV (overwrites existing file)
    with open(out_path, 'w') as csvData:
        writer = csv.writer(csvData)
        header = ['center_freq', 'average', 'start','end','congestion','start','end']
        writer.writerow(header)
    csvData.close()

    def callback(message):
        import ast
        
        # initialize csv fieldvalues
        center_freq = None
        avg_val = avg_start = avg_end = None
        cong_val = cong_start = cong_end = None

        #  (center_freq, {'congestion': [(channel_cong,start_tim,end_time)],
        # 'average': [(channel_avg,start_time,end_time)]})
        current = message.data
        logging.debug('Received message: {}'.format(current)) # moved to end

        try:
            current_dict = ast.literal_eval(message.data)
            center_freq = current_dict.keys()[0]
        except Exception as e:
            print("Failed to convert PubSub message into dictionary. Error: {}"
                    .format(e))

        # Start getting data ready to be saved as CSV
        try:
            average = current_dict[center_freq]['average']
            if average:
                avg_val = average[0][0]
                avg_start = average[0][1]
                avg_end = average[0][2]
        except Exception as e:
            print("Failed to get average values. Error: {}".format(e))
            pass
        try:
            congestion = current_dict[center_freq]['congestion']
            if congestion:
                cong_val = congestion[0][0]
                cong_start = congestion[0][1]
                cong_end = congestion[0][2]
            logging.debug('Average: {}'.format(average))
            logging.debug('Congestion: {}'.format(congestion))
        except Exception as e:
            print("Failed to get congestion values. Error: {}".format(e))
            pass

        csv_row = [center_freq,avg_val,avg_start,avg_end,cong_val,cong_start,cong_end]
        # appends the existing CSV File
        with open(out_path, 'a') as csvData:
            writer = csv.writer(csvData)
            writer.writerow(csv_row)
        csvData.close()         
      
        # if csv is X bytes, close it and create new
        previous = current
        message.ack()

        # print the message to the screen
        print('\n{{\n \"{center_freq}\":\n  {{ \
                \n  \"average\":     {average}, \
                \n  \"congestion\":  {congestion}\n  }} \
                \n}}\n'
                .format(center_freq = center_freq, average = average,
                        congestion=congestion ))

    future = subscriber.subscribe(subscription_name, callback)
    print('Listening for messages on {}.'.format(subscription_name))
    #Blocks the thread while messages are coming in through the stream. Any
    # exceptions that crop up on the thread will be set on the future.
    try:
        # When timeout is unspecified, the result method waits indefinitely.
        future.result()#(timeout=60) # ctrl+shift+\ to kill
    except Exception as e:
        print('Listening for messages on {} threw an Exception: {}.'.format(
                subscription_name, e))  
    

if __name__ == '__main__':
    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()   
        
    # execute the main script    
    run()  
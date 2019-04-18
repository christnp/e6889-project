#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
# Copyright 2019 Nick Christman, W. Aldo Kusmik, & Rashad Barghouti.
# 
# This is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3, or (at your option)
# any later version.
# 
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this software; see the file COPYING.  If not, write to
# the Free Software Foundation, Inc., 51 Franklin Street,
# Boston, MA 02110-1301, USA.
# 
# 

import numpy
from gnuradio import gr

import logging
import threading
import time
import datetime
import pytz
import os
import sys
from google.cloud import pubsub_v1


class google_publisher_py_b(gr.sync_block):
    """
    docstring for block google_publisher_py_b
    This module uses the Google Pub/Sub API to publish GNU Radio data
    to the specified topic. 
    """
    def __init__(self, google_creds, project_id, topic_name, messages_per_sec,attribute):
                
        # set class input arg variables
        self.google_creds = str(google_creds)
        self.project_id = str(project_id)
        self.topic_name = str(topic_name)
        self.message_delay = 1/float(messages_per_sec)
        self.attribute = str(attribute)

        # set the Google Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds

        print("Project ID:    {project_id}\n \
                Topic Name:    {topic_name}\n \
                Message Delay: {messages_per_sec}\n \
                Google Creds:  {google_creds}\n \
                Attribute:     {attribute}\n".format(
                    project_id = self.project_id,
                    topic_name = self.topic_name,
                    messages_per_sec = self.message_delay,
                    google_creds = self.google_creds,
                    attribute = self.attribute))
        sys.exit()
        #set class local variables
        self.dt_format = "%Y-%m-%d %H:%M:%S"
        self.data_count = 0
        self.delay_start = time.time()

        # define PubSub publish timer (used for message frequency)
        self.pub_flag = True # flag used to control frequency of publishing
        # self.publish()

        gr.sync_block.__init__(self,
            name="google_publisher_py_b",
            in_sig=[numpy.float32],
            # in_sig=[numpy.uint8],
            out_sig=None)

    def work(self, input_items, output_items):
        # configuring python logging
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        # define input
        in0 = input_items[0]
        
        # Configure the batch to publish as soon as there is one kilobyte
        # of data or one second has passed.
        # batch_settings = pubsub_v1.types.BatchSettings(
        #     max_bytes=1024,  # One kilobyte
        #     max_latency=1,  # One second
        # )
        # publisher = pubsub_v1.PublisherClient(batch_settings)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_name)

        # publish the data every '1/message_per_sec'
        self.publish(in0) 

        futures = [] # used for PubSub batchsettings
        
        # data_count = 0
        # for i in range(0, len(in0)):
        #     sample = in0[i]
        #     # batch up individual samples   
        #     data = str(sample).encode('utf-8')
        #     print("[{0}] Publishing \'{1}\'!".format(data_count,data))#data))
        #     data_count += 1

            #tmp_future = publisher.publish(topic_path, data=data)            
            #tmp_future = publisher.publish(topic_path, data=data, localdatetime=)
            #futures.append(tmp_future)
            # futures.append(data)

        # TODO: for debugging
        # for future in futures:
            # result() blocks until the message is published.
            # logging.info('message published: {0}'.format(future.result()))
            # logging.info('message published: {0}\n'.format(future))

        return len(input_items[0])
    
    def publish(self,in0):        
        # self.messages_per_sec
        # threading.Timer(2.0, self.publish()).start()
        if time.time() - self.message_delay >= self.delay_start:
            print('its been {} secs'.format(self.message_delay))
            # print("setting pub_flag = True") 
            # self.pub_flag = True
            print("\n\n{0}: len(in0) = {1}".format(self.timestamp(),len(in0)))
            print("[{0}] in0 = {1}".format(self.data_count,in0))
            print("Sum = {0}, Average = {1}".format(sum(in0),sum(in0)/float(len(in0))))
            print("Max = {0}, Min = {1}".format(max(in0),min(in0)))
            self.data_count += 1

            self.delay_start = time.time()              

    def timestamp(self):

        # get current GMT date/time
        # datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        
        # dt_index = datetime.datetime.strptime(tweet_dtc, '%Y-%m-%d %H:%M:%S')
        # convert GMT to NYC time
        # dt_index -= datetime.timedelta(seconds=(60 * 60 * 4))
        # nyc_datetime = dt_index.strftime('%Y-%m-%d-%H-%M-%S')

        # get Eastern Time and return time in desired format
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        return nyc_datetime.strftime(self.dt_format)
        
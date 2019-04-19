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

import numpy
from gnuradio import gr

import logging
import time
import datetime
import pytz
import os
from google.cloud import pubsub_v1

class google_subscriber_py_b(gr.sync_block):
    """
    docstring for block google_subscriber_py_b
    """
    def __init__(self, google_creds, project_id, topic_name,sub_name,snap_name):

       # set class input arg variables
        self.google_creds = str(google_creds)
        self.project_id = str(project_id)
        self.topic_name = str(topic_name)
        self.sub_name = str(sub_name) 
        if str(snap_name) == "None":
            self.snap_name = None
        else:
            self.snap_name = str(snap_name)
        
        
        
        # set the Google Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds

        print("\n \
            Google Creds:       {google_creds}\n \
            Project ID:         {project_id}\n \
            Topic Name:         {topic_name}\n \
            Snapshot Name:      {snap_name}\n".format(                 
                google_creds = self.google_creds,
                project_id = self.project_id,
                topic_name = self.topic_name,
                snap_name = self.snap_name))

        #set class local variables
        self.dt_format = "%Y-%m-%d %H:%M:%S"
        self.data_count = 0

        # Google Pub/Sub configuration
        self.subscriber = pubsub_v1.SubscriberClient()
        self.topic_path = self.subscriber.topic_path(self.project_id, self.topic_name)
        self.sub_path = self.subscriber.subscription_path(self.project_id,self.sub_name)
        self.snap_path = self.subscriber.snapshot_path(self.project_id,self.snap_name)

        # create subscription if it doesn't exist
        try:
            self.subscriber.create_subscription(
                name=self.sub_path, topic=self.topic_path,
                retain_acked_messages=True)
        except:
            self.subscriber.create_snapshot(self.snap_path,self.sub_path)
            print("Snapshot \'{}\' for \'{}\' created\n!".format(self.snap_path,
                    self.sub_path))
                
            self.subscriber.delete_subscription(self.sub_path)
            print("Subscription \'{}\' deleted\n!".format(self.sub_path))

            self.subscriber.create_subscription( name=self.sub_path, 
                        topic=self.topic_path, retain_acked_messages=True)
            print("Subscription \'{}\' created\n!".format(self.sub_path))

        gr.sync_block.__init__(self,
            name="google_subscriber_py_b",
            in_sig=None,
            out_sig=[numpy.uint8])


    def work(self, input_items, output_items):
        # configuring python logging
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        # define output
        out = output_items[0]
        
        
        
        def callback(message):
            # logging.info("\n[{0}] Message #{1} received: \'{3}\' with \
            #             timestamp = \'{4}\'".format(self.timestamp(),self.data_count,
            #             message.data,"holder"))#message.attributes.get('localdatetime')))
            # self.data_count += 1
            # # message is publised as string encoded as UT-8
            # out[:] = numpy.uint8(message.data.decode('utf-8'))            
            print('Received message: {}'.format(message))
            message.ack()

        future = self.subscriber.subscribe(self.sub_path, callback=callback)

        # The subscriber is non-blocking. We must keep the main thread from
        # exiting to allow it to process messages asynchronously in the background.
        # print('Listening for messages on {}'.format(sub_path))
        # while True:
        #     time.sleep(60)
        #Blocks the thread while messages are coming in through the stream. Any
        # exceptions that crop up on the thread will be set on the future.
        try:
            # When timeout is unspecified, the result method waits indefinitely.
            future.result(timeout=60)
        except Exception as e:
            print(
                'Listening for messages on {} threw an Exception: {}.'.format(
                    self.sub_name, e))  
    
        #   <+signal processing here+>
        # out[:] = whatever
        return len(output_items[0])

     # helper function to calculate timestamp
    def timestamp(self):
        # get current GMT date/time
        # nyc_datetime = datetime.datetime.now(datetime.timezone.utc).strftime(self.dt_format)
        
        # get Eastern Time and return time in desired format
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        return nyc_datetime.strftime(self.dt_format)


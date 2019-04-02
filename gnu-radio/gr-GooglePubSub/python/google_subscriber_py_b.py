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
import os
from google.cloud import pubsub_v1

class google_subscriber_py_b(gr.sync_block):
    """
    docstring for block google_subscriber_py_b
    """
    def __init__(self, project_id,topic_name):
        # set the Google Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
        "/home/christnp/Development/e6889/Google/ELEN-E6889-227a1ecc78b6.json"
        # set class variables
        self.project_id = str(project_id)
        self.topic_name = str(topic_name)
        self.sub_name = 'gnradio-sub'
        gr.sync_block.__init__(self,
            name="google_subscriber_py_b",
            in_sig=None,
            out_sig=[numpy.uint8])


    def work(self, input_items, output_items):
        # configuring python logging
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        # define output
        out = output_items[0]
        
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(self.project_id, self.topic_name)
        sub_path = subscriber.subscription_path(self.project_id,self.sub_name)

        # create subscription if it doesn't exist
        try:
            subscriber.create_subscription(
                name=sub_path, topic=topic_path)
        except:
            print("Subscription \'{}\' already exists\n!".format(sub_path))
        
        def callback(message):
            print('Received message: {}'.format(message.data))
            # message is publised as string encoded as UT-8
            out[:] = numpy.uint8(message.data.decode('utf-8'))
            
            message.ack()

        future = subscriber.subscribe(sub_path, callback=callback)

        # The subscriber is non-blocking. We must keep the main thread from
        # exiting to allow it to process messages asynchronously in the background.
        # print('Listening for messages on {}'.format(sub_path))
        # while True:
        #     time.sleep(60)
        #Blocks the thread while messages are coming in through the stream. Any
        # exceptions that crop up on the thread will be set on the future.
        try:
            # When timeout is unspecified, the result method waits indefinitely.
            future.result(timeout=30)
        except Exception as e:
            print(
                'Listening for messages on {} threw an Exception: {}.'.format(
                    self.sub_name, e))  
    
        #   <+signal processing here+>
        # out[:] = whatever
        return len(output_items[0])


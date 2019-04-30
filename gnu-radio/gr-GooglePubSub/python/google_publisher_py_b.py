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
import time
import datetime
import pytz
import os
from google.cloud import pubsub_v1

DT_FORMAT = '%Y-%m-%d %H:%M:%S'

class google_publisher_py_b(gr.sync_block):
    """
    docstring for block google_publisher_py_b
    This module uses the Google Pub/Sub API to publish GNU Radio data
    to the specified topic at a rate of 1/message_delay Hz
    """
    def __init__(self, google_creds, project_id, topic_name, message_delay,
                    center_freq,samp_rate,attribute):
                
        # set class input arg variables
        self.google_creds = str(google_creds)
        self.project_id = str(project_id)
        self.topic_name = str(topic_name)
        self.message_delay = float(message_delay)
        
        # RF characteristics
        self.c_freq = numpy.float32(center_freq)    # center freq (defined)
        self.s_rate = numpy.int32(samp_rate)        # sample rate (defined)
        self.attribute = str(attribute)             # attribute data (select)


        # set the Google Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds

        print("Project ID:      {project_id}\n \
                Topic Name:     {topic_name}\n \
                Message Delay:  {message_delay}\n \
                Google Creds:   {google_creds}\n \
                Center Freq:    {c_freq} \n \
                Samp Rate:      {s_rate} \n \
                Attribute:      {attribute}\n".format( 
                    project_id = self.project_id,
                    topic_name = self.topic_name,
                    message_delay = self.message_delay,
                    google_creds = self.google_creds,
                    c_freq = self.c_freq,
                    s_rate = self.s_rate,
                    attribute = self.attribute))

        #set class local variables
        self.data_count = 0
        self.delay_start = time.time()

        # Google Pub/Sub configuration
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)

        gr.sync_block.__init__(self,
            name="google_publisher_py_b",
            in_sig=[numpy.float32],
            out_sig=None)

    def work(self, input_items, output_items):
        # configuring python logging
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        # define input
        in0 = input_items[0]

        # publish the data every 'message_delay' seconds
        self.publish(in0) 

        return len(input_items[0])
    
    # publish a message every self.message_delay seconds
    # TODO: if we decied to publish raw data as Bytestring, we need to convert
    # numpy.arry (in0) to unicode or bytes via in0.astype("U") or .astype("B")
    def publish(self,in0):    
        
       
        attr_data = 0.0
        if time.time() - self.message_delay >= self.delay_start:
            # calculate the attribute value (chosen in GNU Radio block)
            if (self.attribute == "average"):
                attr_data = sum(in0)/float(len(in0))
            elif (self.attribute == "max"):
                attr_data = max(in0)
            elif (self.attribute == "min"):
                attr_data = min(in0)
            else:
                attr_data = -1.0 # error
                print("No attribute passed to Python code...?\n")
            
            # publish the data to topic and log the event 
            ts_google,ts_nyc = self.timestamp()
            self.publisher.publish(self.topic_path,
                    data=str(attr_data).encode("utf-8"),
                    timestamp =str(ts_google), # use "timestamp" for beam PubSub timestamp_attribute
                    localdatetime=ts_nyc,
                    center_freq = str(self.c_freq),
                    sample_rate = str(self.s_rate))

            logging.info("\n[{0} | {1}] Message #{2} published \'{3}\' = {4}".format(
                            ts_nyc,ts_google,self.data_count,self.attribute,
                            str(attr_data).encode("utf-8")))
         
            # increment global data message counter and reset timer
            self.data_count += 1
            self.delay_start = time.time()              

    # helper function to calculate timestamp
    def timestamp(self):
        # get current GMT date/time
        # nyc_datetime = datetime.datetime.now(datetime.timezone.utc).strftime(DT_FORMAT)
        
        # get Eastern Time and return time in desired format
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))

        #Google PubSub Timestamp in RFC 3339 format, UTC timezone, format
        # Example: 2015-10-29T23:41:41.123Z
        gp_timestamp = str(datetime.datetime.utcnow().isoformat("T")) + "Z"

        # return pubsub timestamp and local (NYC) time in formatted string
        return gp_timestamp, nyc_datetime.strftime(DT_FORMAT)
        
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
import os
from google.cloud import pubsub_v1


class google_publisher_py_b(gr.sync_block):
    """
    docstring for block google_publisher_py_b
    """
    def __init__(self, project_id,topic_name):
        # set the Google Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
        "/home/christnp/Development/e6889/Google/ELEN-E6889-227a1ecc78b6.json"
        # set class variables
        self.project_id = str(project_id)
        self.topic_name = str(topic_name)
        gr.sync_block.__init__(self,
            name="google_publisher_py_b",
            in_sig=[numpy.uint8],
            out_sig=None)


    def work(self, input_items, output_items):
        # configuring python logging
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        # define input
        in0 = input_items[0]
        
        # Configure the batch to publish as soon as there is one kilobyte
        # of data or one second has passed.
        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=1024,  # One kilobyte
            max_latency=1,  # One second
        )
        publisher = pubsub_v1.PublisherClient(batch_settings)
        topic_path = publisher.topic_path(self.project_id, self.topic_name)
        
        futures = []
        for i in range(0, len(in0)):
            sample = in0[i]
            print("Publishing \'{}\'!".format(sample))#data))
            # batch up individual samples
            data = str(sample).encode('utf-8')
            tmp_future = publisher.publish(topic_path, data=data)
            futures.append(tmp_future)

        # TODO: for debugging
        for future in futures:
            # result() blocks until the message is published.
            logging.info('message published: {0}'.format(future.result()))

        return len(input_items[0])


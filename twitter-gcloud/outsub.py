#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Rashad Barghouti (UNI:rb3074)
# Columbia University, New York
# ELEN E6889, Spring 2019
#

# Standard library imports
from __future__ import absolute_import, print_function
import os
import sys
import time
from pprint import pprint

# GC and Beam imports
from google.cloud import pubsub_v1


project = os.getenv('E6889_PROJECT_ID')
sub_name = 'pipeline-output'

subscriber = pubsub_v1.SubscriberClient()
subscription = 'projects/{}/subscriptions/{}'.format(project, sub_name)

def callback(msg):
    print('Msg data: {}'.format(msg.data.decode('utf-8')))
    if msg.attributes:
        print('Msg attributes:')
        pprint(msg.attributes)
    msg.ack()

future = subscriber.subscribe(subscription, callback)

print('Listening for messages on {}'.format(subscription))
try:
    future.result()
except:
    raise

#while True:
#    try:
#        time.sleep(15)
#    except KeyboardInterrupt:
#        print('Got ctrl-c. Stopping.')
#        future.cancel()
#        while not future.cancelled():
#            pass
#        else:
#            print('subscription thread has been shutdown')
#        sys.exit(0)

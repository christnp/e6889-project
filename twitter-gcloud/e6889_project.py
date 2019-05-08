#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Rashad Barghouti (UNI:rb3074)
# Columbia University, New York
# ELEN E6889, Spring 2019
#
#——————————————————————————————————————————————————————————————————————————————
from __future__ import print_function
import time
import logging
from collections import namedtuple
from pprint import pprint, pformat

import tweepy
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

class BaseValidationError(ValueError):
    pass

class ArgumentValueError(BaseValidationError):
    pass

TwitterAccount = namedtuple('TwitterDeveloperAccount',
                            ['api_key',
                            'api_secret_key',
                            'access_token',
                            'access_token_secret'])

class TwitterStreamListener(tweepy.StreamListener):
    """A Twitter stream listener that publishes Tweets to a Pub/Sub topic."""

    def __init__(self, publisher, topic):
        self.publisher = publisher
        self.topic = topic
        self._num = 0
        super(TwitterStreamListener, self).__init__()

    def on_status(self, status):
        """Publish status update to Google Pub/Sub."""
        self._num += 1
        data = status.text
        entities = status.entities
        if status.truncated:
            data = status.extended_tweet['full_text']
            entities = status.extended_tweet['entities']

        # Construct pub message.
        #
        # Reminders:
        #  + The API ‘root-level’ objects that are used to specify a location
        #    in a geo-tagged Tweet are `coordinates` and `place`.
        #  + `place` **is always present** when the Tweet has been geo-tagged.
        #    Since the requests we're sending are filter(geo) type, `place`
        #    should be in all the status updates in received
        #  + `coordinates` **is only present** when the Tweet has been
        #    assigned an exact location
        #  + If `coordinates` is present, it will have the form:
        #    `[longitude, latitude]`
        #  + `place` objects have information about specific, named locations.
        #    (This is like Facebook's "check-in" feature, I think. Twitter
        #    provides a list of Twitter Places to add to the Tweets.)
        #       + So a `place` object present in the Tweet object does not
        #         necessarily mean that the Tweet originated from that
        #         location, only that it is associted with it.
        #       + A Twitter Place is identified by `place_id`.
        #
        data = data.encode('utf-8')
        attrs = {
                # For testing pub/sub
                'e6889_id': u'{}'.format(self._num),
                'id_str': status.id_str,
                # -------------------------------------------------------------
                # 'created_at' is included for readability; 'timestamp_ms' is
                # the attribute used in the beam.ReadFromPubSub()
                #
                # Send the datetime string, not the object.
                # -------------------------------------------------------------
                #'created_at': repr(status.created_at),
                'created_at': u'{}'.format(status.created_at),
                'timestamp_ms': status.timestamp_ms,
                #'truncated': unicode(repr(status.truncated)),
                'lang': status.lang,
                # -------------------------------------------------------------
                # Not sure if this next bunch is useful for anything; ask
                # Nick & Aldo
                # -------------------------------------------------------------
                #'in_reply_to_status_id_str':
                #    status.in_reply_to_status_id_str or 'None',
                #'in_reply_to_user_id_str':
                #    status.in_reply_to_user_id_str or 'None',
                #'in_reply_to_screen_name':
                #    status.in_reply_to_screen_name or 'None',
                #'reply_count': repr(status.reply_count),
                #'retweeted': repr(status.retweeted),
                #'retweet_count': repr(status.retweet_count),

                # 'coordinates' will be None if the Tweet has not been assigned
                # an exact location
                'coordinates': repr(status.coordinates),
                'hashtags': repr(entities['hashtags']),
                'user_mentions': repr(entities['user_mentions']),
                'source': status.source,

                # -------------------------------------------------------------
                # `user` object has been deprecated; go with `author`
                # -------------------------------------------------------------
                'author_id_str': status.author.id_str,
                #'author_name': status.author.name,
                #'author_screen_name': status.author.screen_name,
                #
                # author_location can be None, so wrap it
                'author_location': repr(status.author.location),
                'author_geo_enabled': repr(status.author.geo_enabled),
                #'author_favourites_count':
                #    repr(status.author.favourites_count),
                'author_statuses_count': repr(status.author.statuses_count),
                'author_followers_count': repr(status.author.followers_count),
                'author_friends_count': repr(status.author.friends_count),

                'place_id_str': status.place.id,
                'place_type': repr(status.place.place_type),
                'place_full_name': status.place.full_name,
                'place_country_code': status.place.country_code,
                'place_country': status.place.country,
                'place_bounding_box_type': status.place.bounding_box.type,
                'place_bounding_box_coordinates':
                    repr(status.place.bounding_box.coordinates)
                }

        #attr_str = pformat(attrs)
        #logging.debug('update attributes:\n {}'.format(attr_str))

        future = self.publisher.publish(self.topic, data, **attrs)

        logging.info('**Status update (#{}) published**'.format(self._num))
        logging.info(' e6889_id: {}, created_at: {}, timestamp_ms: {}'.format(
            attrs['e6889_id'],
            attrs['created_at'],
            attrs['timestamp_ms']))
        logging.info((' place_full_name: {}, place_country_code: {}, '
            'place_type: {}, place_id_str: {}').format(
                attrs['place_full_name'],
                attrs['place_country_code'],
                attrs['place_type'],
                attrs['place_id_str']))
        logging.info(' text: {}'.format(data))

    def on_connect(self):
        logging.info('connected to Twitter endpoint')

    def on_error(self, status_code):
        if status_code == 420:
            # Disconnect the stream
            logging.error(('error code {} received from streaming endpoint. '
                ' Disconnecting').format(status_code))
            return False


class TwitterStreamPubSub():

    def __init__(self, acct, project, args):
        """Set up GC Clients and Twitter stream objects."""
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.input_topic = 'projects/{}/topics/{}'.format(
                project,
                args.input_topic_name)
        self.input_sub = 'projects/{}/subscriptions/{}'.format(
                project,
                args.input_sub_name)
        self.output_topic = 'projects/{}/topics/{}'.format(
                project,
                args.output_topic_name)
        self.output_sub = 'projects/{}/subscriptions/{}'.format(
                project,
                args.output_sub_name)

        try:
            self.publisher.create_topic(self.input_topic)
        except AlreadyExists:
            pass
        try:
            self.subscriber.create_subscription(
                    self.input_sub,
                    self.input_topic)
        except AlreadyExists:
            pass

        try:
            self.publisher.create_topic(self.output_topic)
        except AlreadyExists:
            pass
        try:
            self.subscriber.create_subscription(
                    self.output_sub,
                    self.output_topic)
        except AlreadyExists:
            pass

        # Connect to Twitter endpoint
        _auth = tweepy.OAuthHandler(acct.api_key, acct.api_secret_key)
        _auth.set_access_token(acct.access_token, acct.access_token_secret)
        _api = tweepy.API(_auth)
        _listener = TwitterStreamListener(self.publisher, self.input_topic)
        self.Stream = tweepy.Stream(_auth, _listener)

    def filter(self, **kwargs):
        """Start Twitter stream and get Tweets specified by kwargs."""
        self.Stream.filter(**kwargs)
        return

    def subscribe(self, callback=None):
        """Start background subscribe thread."""
        cb = callback or _callback
        return self.subscriber.subscribe(self.input_sub, cb)


def _callback(msg):
    """Process received subscription message."""
    print('subscription message data: ', msg.data.decode('utf-8'))
    if msg.attributes:
        print('subscription message attributes:\n')
        pprint(msg.attributes)
    msg.ack()


# Command-line help string. Too long and obtrusive to have in the parser
# function.
from textwrap import dedent
cmdline_helpstr = dedent(
"""\
Run a Beam pipeline on a stream of geo-tagged Tweets, using DirectRunner or
Google Cloud's Dataflow runner

Twitter API keys — twacct.py
  Twitter API keys are imported from this module, `twacct.py`. Use
  twacct_template.py to add the necessary values

ARGS-FILE:
 @argsfile                  name of file containing option-argument pairs. Such
                            a file can be used (e.g., `gcb.py @args.txt`) to
                            avoid typing long commands lines. Options and
                            arguments listed in the file must be one per line.
                            See argsfile-example.txt

PIPELINE-ARGS:
 --project PROJECT-ID       Google Cloud project ID (required). If not given,
                            an attempt will be made to read it from the
                            environment variable E6889_PROJECT_ID. If that
                            attempt fails, the program will exit with an error
                            message
 --runner RUNNER            pipeline runner. Choices are DataflowRunner or
                            DirectRunner (required, default:{runner})
 --temp_location LOC        GC storage path for temporary workflow data. This
                            is a required argument when the runner is the
                            DirectflowRunner (required,
                            default:{temp_location})
 --input_topic_name [ITPC]  name of the Pub/Sub topic where geo-tagged
                            Tweets will be published (default:
                            {input_topic_name})
 --input_sub_name [ISUB]    name of the Pub/Sub subscription to the input_topic
                            (default: {input_sub_name})
 --output_topic_name [OTPC] name of the Pub/Sub topic where the pipeline output
                            will be puplished (default: {output_topic_name})
 --output_sub_name [OSUB]   name of the Pub/Sub subscription to output_topic
                            (default: {output_sub_name})
""")

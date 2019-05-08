# Title — TBD

# Description — TBD

The implementation uses either a DirectRunner or a Google Cloud DataflowRunner
to execute a Beam pipeline to process geo-tagged tweets from a NYC block
compassing parts of the Columbia University campus

Some of the analytic tasks still in the works are:
1.  Tweet volume fluctuation patters over spans of 24-72 hours
1.  Topic/sentiment detection 
1.  Correlations between above metrics with similar metrics from other data
    sources
1.  Higher workflow loads and corresponding solution/strategies, including
    resource scaling and streaming processing optimization techniques. 

# Code

## Files 

1.  `twbeam.py` — executable
1.  `e6889_project.py` —  project module containing GC and Tweepy subclasses 
1.  `twacct_template.py` — Twitter API keys and access tokens
1.  `argsfile-example.txt` — an example arguments file used to enter cmdline
    option-argument pairs

## Dependencies

1. Pendulum — a timezone/datetime module 
1. Python 2.7 — required for Apache Beam
1. Google Cloud Client libraries for Python
1. Tweepy, a python wrapper for the Twitter API

## Running the Program

*   Use `-h | --help` to get usage information and a listing of required
    arguments.

*   Add the Twitter Developer Account keys and access tokens as specified in
    twacct_cred_template.py

*   Use the `@argsfile` option to run the program. List the option-argument
    pairs you need, one per line, in an text file, e.g., 'args.txt', and use it
    to invoke the program with a command like the following:

    ~~~bash
    $ twbeam.py @args.txt
    ~~~

# Geo-Tagged Tweets and `locations` Request Parameter

    Twitter `locations` spec — using geoJSON's (longitude, latitude) format

        bounding_box = [lon_1, lat_1, lon_2, lat_2]

    (lon_1, lat_1) is the SW corner of the box, and
    (lon_2, lat_2) is the NE corner of the box

    BOUNDING_BOX:
    (lon_1, lat_1) is the corner of Broadway & W 120th St, and
    (lon_2, lat_2) is the corner of Amesterdam Ave & W 135th.

    BOUNDING_BOX_EXTENDED shifts the SW corner to Broadway & W 114th St

    BROADWAY_W120ST = [-73.961967, 40.810504]
    BROADWAY_W114ST = [-73.964766, 40.806667]
    AMESTERDAM_W135ST = [-73.952234, 40.818940]

    BOUNDING_BOX = [-73.961967, 40.810504, -73.952234, 40.818940]
    BOUNDING_BOX_EXTENDED = [-73.964927, 40.807030, -73.952234, 40.818940]


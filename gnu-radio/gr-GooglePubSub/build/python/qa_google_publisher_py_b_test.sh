#!/bin/sh
export VOLK_GENERIC=1
export GR_DONT_LOAD_PREFS=1
export srcdir=/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python
export PATH=/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python:$PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PYTHONPATH=/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/swig:$PYTHONPATH
/usr/bin/python2 /home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python/qa_google_publisher_py_b.py 

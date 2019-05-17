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

from gnuradio import gr, gr_unittest
from gnuradio import blocks
from google_publisher_py_b import google_publisher_py_b
from google_subscriber_py_b import google_subscriber_py_b

class qa_google_publisher_py_b (gr_unittest.TestCase):

    def setUp (self):
        self.tb = gr.top_block ()

    def tearDown (self):
        self.tb = None

    def test_001_t (self):
        project_id = 'elen-e6889'
        topic_name = 'gnuradio'
        src_data = (1,2,3,4)
        expected_result = src_data
        # Google publisher        
        src = blocks.vector_source_b (src_data)
        pub = google_publisher_py_b (project_id,topic_name)
        self.tb.connect (src, pub)
        # Google subscriber
        sub = google_subscriber_py_b (project_id,topic_name)
        dst = blocks.vector_sink_b ()
        self.tb.connect (sub, dst)
        # run the test
        self.tb.run ()
        # check the results
        result_data = dst.data ()
        self.assertFloatTuplesAlmostEqual (expected_result, result_data, 6)


if __name__ == '__main__':
    gr_unittest.run(qa_google_publisher_py_b, "qa_google_publisher_py_b.xml")

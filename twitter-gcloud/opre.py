#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
"""Operator reordering optimization.

E6889: Homework 2
Rashad Barghouti (rb3074)
"""

from __future__ import print_function
import sys
import csv
import time
import argparse
import logging


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToText
from apache_beam.pvalue import AsDict
from apache_beam.options.pipeline_options import PipelineOptions

# Program arguments — move to arg_parser
#
stream_fname = 'vs3.csv'
patient_db_fname = 'pd3.csv'
# Nonreordered and Reoredered output files
nre_fname = 'nre-data.txt'
reo_fname = 'reo-data.txt'
do_plot = True
if do_plot:
    import matplotlib.pyplot as plt

RUNNER = 'DirectRunner'
MAX_RISK = 1000

class SelectInputFn(beam.DoFn):
    """Selects input data based on value of selectivity parameter."""

    def process(self, element, selectivity):
        risk_lower_bound = MAX_RISK - int(selectivity * MAX_RISK)
        if element[1] >= risk_lower_bound:
            yield element


class SelectOutputFn(beam.DoFn):
    """Forwards output data based on selectivity parameter."""

    def process(self, element, selectivity):
        risk_lower_bound = MAX_RISK - int(selectivity * MAX_RISK)
        if element[1][3] >= risk_lower_bound:
            yield element


class GetVitalsFn(beam.DoFn):
    """Splits input line to return a 2-tuple of received patient data.

    Output:
        (patient_id, vital_signs_value).
    """

    def process(self, element):
        d = element.split(',')
        yield int(d[0]), int(d[1])


class GetPatientRecordFn(beam.DoFn):
    """Splits input line to return a 2-tuple patient record.

    Output:
        patient_record = (id, [name, year_of_birth, gender])
    """

    def process(self, element):
        r = element.split(',')
        yield int(r[0]), [str(r[1]), int(r[2]), str(r[3])]


#class FormatPerfDataFn(beam.DoFn):
#    def process(self, element, tm):
#        return ['{} {}'.format(element, round(tm, 2))]


class CollectPerfDataFn(beam.DoFn):
    """Collects performance data for Op-reordered processing."""

    def process(self, element, tm, perflist):
        s = '{} {}'.format(element, round(tm, 2))
        perflist.append(s)
        return [s]


class PrintFn(beam.DoFn):
    """Utility print function for testing on DirectRunner."""

    def __init__(self, label=None):
        self.label = label

    def process(self, element):
        if self.label is None:
            print('{}'.format(element))
        else:
            print('{}: {}'.format(self.label, element))


#——————————————————————————————————————————————————————————————————————————————
def _main():

    # Non-reordered (NR) processing
    ofname = nre_fname
    NRperf = [0.5]
    with beam.Pipeline(runner=RUNNER) as p:
        nre_tm = time.clock()

        vsd = (p | 'Stream file' >> ReadFromText(stream_fname)
                 | 'Vital signs data' >> beam.ParDo(GetVitalsFn()))
        rec = (p | 'Patient db' >> ReadFromText(patient_db_fname)
                 | 'Patient records' >> beam.ParDo(GetPatientRecordFn()))

        # Beam CoGroupByKey() does an outer join; need inner equi-join
        def inner_join(vsd, rec):
            key, val = vsd
            return key, rec[key] + [val]

        # Set operator A selectivity to 0.5; enrich streamed data;
        pnt_coll = (vsd | 'NR select' >> beam.ParDo(SelectInputFn(), 0.5)
                        | 'NR join' >> beam.Map(inner_join, AsDict(rec)))

        # Op-B: forward all data but run it through selection process
        send_coll = pnt_coll | 'NR send' >> beam.ParDo(SelectOutputFn(), 1.0)
        ocount = send_coll | 'NR count' >> beam.combiners.Count.Globally()

        ocount | 'NR write text' >> WriteToText(ofname, shard_name_template='')

    # Wait here for all to finish
    nre_tm = time.clock() - nre_tm
    with open(ofname, 'r') as of:
        n_output_tuples = int(of.readline())
        NRperf.append(n_output_tuples)

    NRperf.append(round(nre_tm, 4))
    norm = n_output_tuples/nre_tm

    # Operator-reordered (RE) processing
    ofname = reo_fname
    REperflist = []
    sBlist = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    for i, sB in enumerate(sBlist):
        s = 'RE' + str(i) + ' '
        # Store selectivity, which is 1.0 - sB
        REperf = [round((1.0-sB), 1)]
        with beam.Pipeline(runner=RUNNER) as p:
            tm = time.clock()
            vsd = (p | s + 'stream' >> ReadFromText(stream_fname)
                     | s + 'stream data' >> beam.ParDo(GetVitalsFn()))
            rec = (p | s + 'Patient db' >> ReadFromText(patient_db_fname)
                     | s + 'Patient recs' >> beam.ParDo(GetPatientRecordFn()))

            def inner_join(vsd, rec):
                key, val = vsd
                return key, rec[key] + [val]

            send_pcoll = (vsd | s + 'select' >> beam.ParDo(SelectInputFn(), sB)
                              | s + 'join' >> beam.Map(inner_join,
                                                        AsDict(rec)))

            ocount = (send_pcoll | s+'count' >> beam.combiners.Count.Globally()
                                 | s + 'write text' >> WriteToText(ofname,
                                                shard_name_template=''))

        # Store performance data, including normalized thruput for this run
        tm = time.clock() - tm
        with open(ofname, 'r') as of:
            n_output_tuples = int(of.readline())
            REperf.append(n_output_tuples)
        REperf.append(round(tm, 4))
        REperf.append(round(n_output_tuples/tm/norm, 2))
        REperflist.append(REperf)

    # Write data to local files
    #print('{}'.format(REperflist))
    write_output_to_files(NRperf, REperflist)

    if do_plot == True:
        plot_throughput(REperflist)



def write_output_to_files(NRperf, REperflist):
    """Writes output summaries to text files."""

    glbl_winsz = data_size(stream_fname)
    ptbl_sz = data_size(patient_db_fname)
    with open(nre_fname, 'w') as nf, open(reo_fname, 'w') as rf:

        s = '* Stream IO/global window size: {}'.format(glbl_winsz)
        print('* Non-reordered processing data', s, sep='\n', file=nf)
        print('* Reordered processing data', s, sep='\n', file=rf)

        s = '* Patient db/right join tbl size): {} records'.format(ptbl_sz)
        print(s, file=nf)
        print(s, file=rf)

        s = '\n[selectivity, # output tuples, processing time (sec)]'
        print(s, file=nf)
        print('{}'.format(NRperf), file=nf)
        s = ['\n[selectivity, # output tuples, processing time (sec),']
        s += ['normalized througput]']
        s = ' '.join(s)
        print(s, file=rf)
        print(s)

        for perf in REperflist:
            s = '{}'.format(perf)
            print(s, file=rf)
            print(s)


def plot_throughput(reo_data):

    nre_thruput = [1 for i in range(10)]
    reo_thruput = [t[3] for t in reo_data]
    selectivity = [s[0] for s in reo_data]
    plt.figure()
    plt.title("Operator Reordering Optimization", size='large')
    plt.xlabel('Selectivity', size='large')
    plt.ylabel('Normalized Throughput', size='large')

    plt.plot(selectivity, nre_thruput, 'r', scalex=False, label='Nonreordered')
    plt.plot(selectivity, reo_thruput, 'b', scalex=False, dashes=[6, 4],
            label='Reordered')

    plt.legend(loc='best', shadow=True, fontsize='medium', facecolor='#FFFFFF')
    plt.grid(True)
    plt.ylim(top=2.5)
    plt.xlim(right=0.9)
    plt.savefig('thruput-plot.png')
    #plt.show()


def data_size(fname):

    # no way around counting all lines
    with open(fname, 'r') as f:
        for i, l in enumerate(f, start=1):
            pass
    return i


if __name__ == '__main__':

    if RUNNER == 'DirectRunner':
        logging.getLogger().setLevel(logging.ERROR)
    _main()


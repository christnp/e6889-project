~~~bash
$ grpl.py @args.txt
/usr/lib/python2.7/site-packages/requests/__init__.py:91: RequestsDependencyWarning: urllib3 (1.25.2) or chardet (3.0.4) doesn't match a supported version!
  RequestsDependencyWarning)
INFO:root:Google Pub/Sub topic: 'projects/e6889-236006/topics/gnuradio'
INFO:root:Google Pub/Sub subscription: 'projects/e6889-236006/subscriptions/gnuradio-sub'

INFO:root:Starting GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/pipeline.pb...
INFO:oauth2client.transport:Attempting refresh to obtain initial access_token
INFO:oauth2client.client:Refreshing access_token
INFO:oauth2client.transport:Attempting refresh to obtain initial access_token
INFO:oauth2client.client:Refreshing access_token
INFO:root:Completed GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/pipeline.pb in 0 seconds.
INFO:root:Starting GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/pickled_main_session...
INFO:root:Completed GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/pickled_main_session in 0 seconds.
INFO:root:Downloading source distribtution of the SDK from PyPi
INFO:root:Executing command: ['/usr/bin/python2', '-m', 'pip', 'download', '--dest', '/tmp/tmpHDF7yC', 'apache-beam==2.11.0', '--no-deps', '--no-binary', ':all:']
/usr/lib/python2.7/site-packages/requests/__init__.py:91: RequestsDependencyWarning: urllib3 (1.25.2) or chardet (3.0.4) doesn't match a supported version!
  RequestsDependencyWarning)
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7.
INFO:root:Staging SDK sources from PyPI to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/dataflow_python_sdk.tar
INFO:root:Starting GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/dataflow_python_sdk.tar...
INFO:root:Completed GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/dataflow_python_sdk.tar in 0 seconds.
INFO:root:Downloading binary distribtution of the SDK from PyPi
INFO:root:Executing command: ['/usr/bin/python2', '-m', 'pip', 'download', '--dest', '/tmp/tmpHDF7yC', 'apache-beam==2.11.0', '--no-deps', '--only-binary', ':all:', '--python-version', '27', '--implementation', 'cp', '--abi', 'cp27mu', '--platform', 'manylinux1_x86_64']
/usr/lib/python2.7/site-packages/requests/__init__.py:91: RequestsDependencyWarning: urllib3 (1.25.2) or chardet (3.0.4) doesn't match a supported version!
  RequestsDependencyWarning)
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7.
INFO:root:Staging binary distribution of the SDK from PyPI to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/apache_beam-2.11.0-cp27-cp27mu-manylinux1_x86_64.whl
INFO:root:Starting GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/apache_beam-2.11.0-cp27-cp27mu-manylinux1_x86_64.whl...
INFO:root:Completed GCS upload to gs://e6889/staging/e6889-project-20190511-092801.1557581282.025266/apache_beam-2.11.0-cp27-cp27mu-manylinux1_x86_64.whl in 0 seconds.
INFO:root:Create job: <Job
 createTime: u'2019-05-11T13:28:08.027589Z'
 currentStateTime: u'1970-01-01T00:00:00Z'
 id: u'2019-05-11_06_28_06-7740294521817169044'
 location: u'us-central1'
 name: u'e6889-project-20190511-092801'
 projectId: u'e6889-236006'
 stageStates: []
 startTime: u'2019-05-11T13:28:08.027589Z'
 steps: []
 tempFiles: []
 type: TypeValueValuesEnum(JOB_TYPE_STREAMING, 2)>
INFO:root:Created job with id: [2019-05-11_06_28_06-7740294521817169044]
INFO:root:To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow/jobsDetail/locations/us-central1/jobs/2019-05-11_06_28_06-7740294521817169044?project=e6889-236006
INFO:root:Job 2019-05-11_06_28_06-7740294521817169044 is in state JOB_STATE_PENDING
INFO:root:2019-05-11T13:28:10.431Z: JOB_MESSAGE_DETAILED: Checking permissions granted to controller Service Account.
INFO:root:2019-05-11T13:28:10.921Z: JOB_MESSAGE_BASIC: Worker configuration: n1-standard-4 in us-central1-a.
INFO:root:2019-05-11T13:28:11.363Z: JOB_MESSAGE_DETAILED: Expanding SplittableParDo operations into optimizable parts.
INFO:root:2019-05-11T13:28:11.365Z: JOB_MESSAGE_DETAILED: Expanding CollectionToSingleton operations into optimizable parts.
INFO:root:2019-05-11T13:28:11.372Z: JOB_MESSAGE_DETAILED: Expanding CoGroupByKey operations into optimizable parts.
INFO:root:2019-05-11T13:28:11.385Z: JOB_MESSAGE_DETAILED: Expanding SplittableProcessKeyed operations into optimizable parts.
INFO:root:2019-05-11T13:28:11.387Z: JOB_MESSAGE_DETAILED: Expanding GroupByKey operations into streaming Read/Write steps
INFO:root:2019-05-11T13:28:11.396Z: JOB_MESSAGE_DEBUG: Annotating graph with Autotuner information.
INFO:root:2019-05-11T13:28:11.409Z: JOB_MESSAGE_DETAILED: Fusing adjacent ParDo, Read, Write, and Flatten operations
INFO:root:2019-05-11T13:28:11.412Z: JOB_MESSAGE_DETAILED: Unzipping flatten s18 for input s16.out
INFO:root:2019-05-11T13:28:11.415Z: JOB_MESSAGE_DETAILED: Fusing unzipped copy of GroupAll/GroupByKey/WriteStream, through flatten GroupAll/Flatten, into producer GroupAll/pair_with_average
INFO:root:2019-05-11T13:28:11.417Z: JOB_MESSAGE_DETAILED: Fusing consumer CreateTupleAverage into ParseData
INFO:root:2019-05-11T13:28:11.420Z: JOB_MESSAGE_DETAILED: Fusing consumer SignalDetector/PeakFilter into ParseData
INFO:root:2019-05-11T13:28:11.422Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAll/GroupByKey/WriteStream into GroupAll/pair_with_congestion
INFO:root:2019-05-11T13:28:11.424Z: JOB_MESSAGE_DETAILED: Fusing consumer SignalDetector/CreateTupleFreq into SignalDetector/PeakFilter
INFO:root:2019-05-11T13:28:11.427Z: JOB_MESSAGE_DETAILED: Fusing consumer SignalDetector/PeakMarker into SignalDetector/PeakFilter
INFO:root:2019-05-11T13:28:11.430Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupCongestion/WriteStream into WindowCongestion
INFO:root:2019-05-11T13:28:11.433Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAll/pair_with_average into ChannelAverage
INFO:root:2019-05-11T13:28:11.435Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAverage/WriteStream into WindowAverage
INFO:root:2019-05-11T13:28:11.437Z: JOB_MESSAGE_DETAILED: Fusing consumer SignalDetector/DebugOutput into SignalDetector/CreateTupleFreq
INFO:root:2019-05-11T13:28:11.439Z: JOB_MESSAGE_DETAILED: Fusing consumer FormatPubSub into GroupAll/Map(_merge_tagged_vals_under_key)
INFO:root:2019-05-11T13:28:11.441Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAll/Map(_merge_tagged_vals_under_key) into GroupAll/GroupByKey/MergeBuckets
INFO:root:2019-05-11T13:28:11.443Z: JOB_MESSAGE_DETAILED: Fusing consumer ChannelAverage into GroupAverage/MergeBuckets
INFO:root:2019-05-11T13:28:11.445Z: JOB_MESSAGE_DETAILED: Fusing consumer ChannelCongestion into GroupCongestion/MergeBuckets
INFO:root:2019-05-11T13:28:11.446Z: JOB_MESSAGE_DETAILED: Fusing consumer GetData/Map(_from_proto_str) into GetData/Read
INFO:root:2019-05-11T13:28:11.448Z: JOB_MESSAGE_DETAILED: Fusing consumer WindowCongestion into SignalDetector/CreateTupleCount
INFO:root:2019-05-11T13:28:11.450Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAverage/MergeBuckets into GroupAverage/ReadStream
INFO:root:2019-05-11T13:28:11.452Z: JOB_MESSAGE_DETAILED: Fusing consumer WriteToPubSub/Write/NativeWrite into FormatPubSub
INFO:root:2019-05-11T13:28:11.453Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAll/pair_with_congestion into ChannelCongestion
INFO:root:2019-05-11T13:28:11.455Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupAll/GroupByKey/MergeBuckets into GroupAll/GroupByKey/ReadStream
INFO:root:2019-05-11T13:28:11.457Z: JOB_MESSAGE_DETAILED: Fusing consumer GroupCongestion/MergeBuckets into GroupCongestion/ReadStream
INFO:root:2019-05-11T13:28:11.459Z: JOB_MESSAGE_DETAILED: Fusing consumer SignalDetector/CreateTupleCount into SignalDetector/PeakMarker
INFO:root:2019-05-11T13:28:11.461Z: JOB_MESSAGE_DETAILED: Fusing consumer ParseData into GetData/Map(_from_proto_str)
INFO:root:2019-05-11T13:28:11.463Z: JOB_MESSAGE_DETAILED: Fusing consumer WindowAverage into CreateTupleAverage
INFO:root:2019-05-11T13:28:11.473Z: JOB_MESSAGE_DEBUG: Adding StepResource setup and teardown to workflow graph.
INFO:root:2019-05-11T13:28:11.534Z: JOB_MESSAGE_DEBUG: Adding workflow start and stop steps.
INFO:root:2019-05-11T13:28:11.584Z: JOB_MESSAGE_DEBUG: Assigning stage ids.
INFO:root:2019-05-11T13:28:11.691Z: JOB_MESSAGE_DEBUG: Executing wait step start2
INFO:root:Job 2019-05-11_06_28_06-7740294521817169044 is in state JOB_STATE_RUNNING
INFO:root:2019-05-11T13:28:11.701Z: JOB_MESSAGE_DEBUG: Starting worker pool setup.
INFO:root:2019-05-11T13:28:11.705Z: JOB_MESSAGE_BASIC: Starting 1 workers...
INFO:root:2019-05-11T13:28:12.846Z: JOB_MESSAGE_DETAILED: Pub/Sub resources set up for topic 'projects/e6889-236006/topics/gnuradio'.
INFO:root:2019-05-11T13:28:14.360Z: JOB_MESSAGE_BASIC: Executing operation GroupAll/GroupByKey/ReadStream+GroupAll/GroupByKey/MergeBuckets+GroupAll/Map(_merge_tagged_vals_under_key)+FormatPubSub+WriteToPubSub/Write/NativeWrite
INFO:root:2019-05-11T13:28:14.360Z: JOB_MESSAGE_BASIC: Executing operation GroupAverage/ReadStream+GroupAverage/MergeBuckets+ChannelAverage+GroupAll/pair_with_average+GroupAll/GroupByKey/WriteStream
INFO:root:2019-05-11T13:28:14.360Z: JOB_MESSAGE_BASIC: Executing operation GroupCongestion/ReadStream+GroupCongestion/MergeBuckets+ChannelCongestion+GroupAll/pair_with_congestion+GroupAll/GroupByKey/WriteStream
INFO:root:2019-05-11T13:28:14.360Z: JOB_MESSAGE_BASIC: Executing operation GetData/Read+GetData/Map(_from_proto_str)+ParseData+CreateTupleAverage+SignalDetector/PeakFilter+SignalDetector/CreateTupleFreq+SignalDetector/PeakMarker+SignalDetector/DebugOutput+SignalDetector/CreateTupleCount+WindowCongestion+GroupCongestion/WriteStream+WindowAverage+GroupAverage/WriteStream
INFO:root:2019-05-11T13:29:07.625Z: JOB_MESSAGE_DETAILED: Workers have started successfully.
INFO:root:2019-05-11T13:29:08.069Z: JOB_MESSAGE_DEBUG: Executing input step topology_init_attach_disk_input_step
~~~

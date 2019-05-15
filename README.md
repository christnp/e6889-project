# e6889-project

## Beam Pipeline Resources:
TBD

## Google PubSub and Dataflow Resources:
TBD

## Twitter Resrouces:
TBD

## Camera Resources:
TBD

## Weather Resources:
TBD

## SDR Resources:
This section provides important information for the SDR applicaiton.

### GNU Raidio Setup (Linux)
The GNU Radio source code is located in the gnu-radio directory. To install the custom google-pubsub OOT, use the following process:

0. Install the SDR module for GNU Radio (e.g., gr-osmosdr via apt-get install osmosdr) and test in GNU Radio
1. Navigate to gnuradio/gr-GooglePubSub GNU Radio module
2. Make a "build" director ancd "cd build/" (if build directory exists, move or delete it).
3. Execute the following to build the module and add to GNU Radio Companion (GRC):
    a. cmake ../
       NOTE: if PyBOMBS was used to install GNU Radio, then: 
       cmake -DCMAKE_INSTALL_PREFIX= ../ #  should be the configured PyBOMBS target
    b. make
    c. sudo make install
    d. sudo ldconfig
4. Make sure you have the correct Google SDK installed to run Google PubSub (Python)
5. Start GRC and select File > Open
6. Navigate to and open gnu-radio/grc-workspace/rf-scanner-pubsub.grc
7. Execute the workflow, looking for any errors in the terminal display
* Most common errors have to do with the installation of GNU radio and dependencies.

### GNU Radio References
https://wiki.gnuradio.org/index.php/TutorialsCoreConcepts
https://wiki.gnuradio.org/index.php/Guided_Tutorial_GNU_Radio_in_Python

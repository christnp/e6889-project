# GNU Radio Readme

This document provides information on setting up the GNU radio environment and using the provided code. 

__NOTE:__ As of 05/16/2019, the grpl.py and ulpl.py are independent Python files. Changes in grpl.py are not reflected in ullpl.py, and vice-versa.

## GNU Radio Setup (Linux)

### Step 1:
The first step in the process is to install and configure GNU Radio. The easiest way to install GNU Radio is to use the pre-built binaries: `sudo apt-get install gnuradio`.

Alternatively, follow the instructions provided on the GNU Radio website: https://wiki.gnuradio.org/index.php/InstallingGR

### Step 2:
Next, install the module that supports the target SDR. For example, the Nooelec SmarTee radio used for the subject project utilizes the OSMOCOM RTL-SDR block found in gr-osmosdr OOT module. The easiest way to install the module is to use the pre-build binaries: `sudo apt-get install gr-osmosdr`.

Alternatively, follow the instructions provided on the Github site: https://github.com/osmocom/gr-osmosdr

Note: pay attention during the make and make install processes and ensure that the RTL-SDR is enabled after compilation (it may require additional dependencies)

### Step 3:
Finally, to install the custom google-pubsub OOT module, use the following process:

1. Navigate to `gnuradio_source/gr-GooglePubSub` GNU Radio OOT module directory
2. Make a `build/` directory and `cd build/` (if build directory exists, move or delete it and create a new one).
3. From within the `build/` directory, execute the following to build the module and add to GNU Radio Companion (GRC):
    * `cmake ../` <br />
        Note: if PyBOMBS was used to install GNU Radio, then use `cmake -DCMAKE_INSTALL_PREFIX= ../` #  should be the configured PyBOMBS target <br />
    * `make`
    * `sudo make install`
    * `sudo ldconfig`
4. Make sure you have the correct Google SDK installed to run Google PubSub (Python)
    a. `pip install google-cloud-storage`
5. Start GRC and select `File > Open`
6. Navigate to and open `gnuradio_source/grc-workspace/google-pubsub.grc`
7. Execute the workflow, looking for any errors in the terminal display <br />
    Note: Most common errors have to do with the installation of GNU radio and dependencies. <br />
8. Alternatively execute the python script `gnuradio_source/grc-workspace/rf-scanner_pubsub.py`

### GNU Radio References
https://wiki.gnuradio.org/index.php/TutorialsCoreConcepts <br />
https://wiki.gnuradio.org/index.php/Guided_Tutorial_GNU_Radio_in_Python


## Google Dataflow Pipeline

`python grpl.py @args.txt` <br />

Use the command above to execute the standalone GNU Radio pipeline on Google Cloud Dataflow. The `args.txt` file will need to be updated with the correct Google Cloud credentials and PubSub parameters. The GNU Radio signal processing application (detailed above) will also need to be properly configured with the Google PubSub information and executed so that the Beam pipeline has an input data source.

__TODO:__ Make this a class and import into ulpl.py

## Utilities

Included in the `gnuradio_source/utilities` directory is a subscriber script that can be used to collect the data from Google PubSub and save the results to a CSV file. The script will need to be modified to use the correct Google Cloud credentials, topics and subscriptions.


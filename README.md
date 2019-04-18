# e6889-project


## SDR Resources:

Typical EMS/PD Radio: P25 (~Astro 25)
P25 Freq. Range: 450-482MHz
P25 Channel B/W: Phase 1, FDMA 12.5kHz & Phase 2, TDMA 6.25kHz

Using Qniglo Q-168 Kids Walkie-Talkie for Test
Q-168 Freq. Range: 462-467MHz
Q-168 Channels: 22
Q-168 Channel B/W:

GNU Radio Defaults:
1. google_publisher_py_b
- Google Creds: "/home/christnp/Development/e6889/Google/ELEN-E6889-227a1ecc78b6.json"
- Project ID: ""
- Topic Name: ""
- Message/Sec: 1 
NY Police Frequencies: https://www.radioreference.com/apps/db/?ctid=1855
https://wiki.gnuradio.org/index.php/TutorialsCoreConcepts
https://wiki.gnuradio.org/index.php/Guided_Tutorial_GNU_Radio_in_Python

peak_detector_fb(float threshold_factor_rise = 0.25, float threshold_factor_fall = 0.40, int look_ahead = 10, float alpha = 0.001) 
peak_detector_sb
"Keep in mind that the gr_peak_detector actually expects negative
inputs. So if your signal goes from 0 to 100, adjust it so that it
goes from -100 to 0.

Of course, I say "keep in mind" even though we probably haven't
provided any documentation in the code to that affect..."


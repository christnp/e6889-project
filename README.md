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
0. Install RTL-SDR and test in GNU Radio
1. Navigate to gr-GooglePubSub GNU Radio module
2. Make a "build" directory (cd build)
3. execute the following to build the module and add to GNU Radio Companion (GRC):
    a. cmake ../
    b. make
    c. sudo make install
    d. sudo ldconfig
4. Start GRC and select File > Open
5. Navigate to and open gnu-radio > grc-workspace > google-pubsub.grc
6. Execute the workflow, looking for any errors in the terminal display

### EMS/PD/FD Radio
Typical EMS/PD Radio: P25 (~Astro 25)
P25 Freq. Range: 450-482MHz
P25 Channel B/W: Phase 1, FDMA 12.5kHz & Phase 2, TDMA 6.25kHz

### Expect RF Environment
Columbia U. (Mudd) is in NYPD 26th Precinct
NYC/Manhattan PD/FD Frequencies: https://www.radioreference.com/apps/db/?ctid=1855
* 476.63750  WIF537  RM  186.2 PL  NYPD MN 25/28/32  Precincts 25, 28, 32  FM  Law Dispatch
* 476.36250  WIF538  RM  100.0 PL  NYPD MN 26/30	   Precincts 26, 30 	 FM  Law Dispatch
* 482.10625  WQFH238 	B	146.2 PL	FDNY MN Dispatch  Manhattan Dispatch  FMN   Fire Dispatch 
* 485.10625  WQFH239 	M		        FDNY MN Mobiles	  Manhattan Mobiles  FMN 	Fire Dispatch
* 482.98125  WQFH238 	RM	156.7 PL	EMS MN C Disp	Manhattan Central Dispatch 	FMN 	EMS Dispatch 
* 482.75625  WQFH238 	RM	151.4 PL	EMS MN N Disp	Manhattan North Dispatch 	FMN 	EMS Dispatch 
* 483.21875  WQFH238 	RM	162.2 PL	EMS MN S Disp	Manhattan South Dispatch 	FMN 	EMS Dispatch 
* 483.28125  WQFH239 	M	167.9 PL	EMS MN C Tac	Manhattan Central Tactical 	FMN 	EMS-Tac 
* 483.03125  WQFH239 	M	167.9 PL	EMS MN N Tac	Manhattan North Tactical 	FMN 	EMS-Tac 
* 483.29375  WQFH239 	M	167.9 PL	EMS MN S Tac	Manhattan South Tactical 	FMN 	EMS-Tac   

### Test RF Environment
Using Qniglo Q-168 Kids Walkie-Talkie for Test
Q-168 Freq. Range: 462-467MHz
Q-168 Channels: 22
Q-168 Channel B/W: ??

### GNU Radio References
https://wiki.gnuradio.org/index.php/TutorialsCoreConcepts
https://wiki.gnuradio.org/index.php/Guided_Tutorial_GNU_Radio_in_Python

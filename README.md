G1 GC log truncater

This utility was written with intention of truncating 
logs in between particular time frame so that it will 
be easier to run them through gc_viewer or any other 
similar softwares. 

Also this utility was written keeping in mind that these
logs are going to be very beefy and hence partitioning
and seeking is done to speed up the process, in general
for files around 5GB this completes in 10 seconds if 
the timesize you are truncating is with in 1-2 days



## Installation

Install lein and java 7 and then clone the repo
and use via the Usage

uber jar will be available shortly

## Usage

    $ lein run -m <source-file> <target-file> <start-date-in-iso> <end-date-in-iso>




Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

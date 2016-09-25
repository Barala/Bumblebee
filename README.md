# Bumblebee [Generate SSTables for given token ranges]
[![Build Status](https://travis-ci.org/Barala/Bumblebee.svg?branch=master)](https://travis-ci.org/Barala/Bumblebee)
## Purpose ::
	* No need to use sstableLoader for bulk data [It consumes more cpu usage and creates many sstables]
	* So this tool can be used while restoring the data 


## Working ::
	** For given target cluster It calculates the token range for given keyspace
	** Then it generates the set of all unique ranges
	** Read sstable one by one and based on above set of unique range write them back to sstables


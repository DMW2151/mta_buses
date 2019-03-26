#!/bin/bash
names='http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip
		http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip
		http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip
		http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip
		http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'

for name in $names
do
	# Assign filename
	fp=$(basename $name)
	# Download (and unzip) .zip files using curl
	curl -o $fp $name
done
# mta_buses

This repo contains a subset of the code in my ongoing work on MTA performance statistics. This repo primarily contains code used to setup 
and read data into a postgres database.

**Files**
- batch_performance_processes.py - performs nightly summary of all bus trips for the prior day; saves to S3 and archives data older than 2 weeks.<br/><br/>
- rds_aws_conn.py - utility function, allows connection to AWS RDS instance<br/><br/>
- read_gtfs_feed.py - reads from the MTA GTFS API<br/><br/>
- s3_aws_conn.py - utility function, allows connection to AWS S3 instance<br/><br/>

- ./StaticData/local_download_mta_data.sh - downloads a zip file containing static data (available at the MTA developers website) to local drive<br/><br/>
- ./StaticData/local_import_geo_data.py - performs basic distance calculations on MTA static data and uploads to RDS<br/><br/>
- ./StaticData/local_import_trip_data.py - uploads reference tables from MTA static data to RDS<br/><br/>

### To-Do

1. Integrate the postgres database w. postgrest and expose the api
2. Include calculations for speed estimates

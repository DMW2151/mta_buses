# mta_buses

This repo contains a subset of the code in my ongoing work on MTA performance statistics. This repo primarily contains code used to setup 
and read data into a postgres database.

**Files**
- batch_performance_processes.py - performs nightly summary of all bus trips for the prior day; saves to S3 and archives data older than 2 weeks. The database grows by about 2-3GB/day depending on day of the week and frequency of gtfs api calls.

- rds_aws_conn.py - utility function, allows connection to AWS RDS instance
- read_gtfs_feed.py - reads from the MTA GTFS API
- s3_aws_conn.py - utility function, allows connection to AWS S3 instance
- ./StaticData/local_download_mta_data.sh - downloads a zip file containing static data (available at the MTA developers website) to local drive
- ./StaticData/local_import_geo_data.py - performs basic distance calculations on MTA static data and uploads to RDS
- ./StaticData/local_import_trip_data.py - uploads reference tables from MTA static data to RDS

### To-Do

1. Integrate the postgres database w. postgrest and expose the api
2. Include calculations for speed estimates

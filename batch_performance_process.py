# -*- coding: utf-8 -*-
"""
Contains all code for batch processes, script runs on EC2 instance, scheduled
for 4am;

1) Calculates the total time of each bus-trip from the previous
2) Day sends summary calcs to S3
3) Removes data beyond 14 days old
"""
import numpy as np
import pandas as pd
from datetime import datetime

from rds_aws_conn import conn
from s3_aws_conn import s3conn, BUCKET, contents_to_s3

# Returns direction, trip id, trip start date, start time (first time seen),
# end time (last time seen), and runtime (difference between first and last time seen).
ARCHIVE_DB_SQL = """
                SELECT *, (subq.end_time - subq.start_time) * '1 second'::interval AS runtime
                FROM (
                SELECT vehicle_trip_direction_id, vehicle_trip_trip_id,
                        %s AS vehicle_trip_start_date,
                        MIN(vehicle_timestamp) AS start_time,
                        MAX(vehicle_timestamp) AS end_time FROM bus_locations
                WHERE vehicle_trip_start_date = %s
                GROUP BY vehicle_trip_direction_id, vehicle_trip_trip_id
                ) AS subq;
                """
                
# Removes observations from data older than 14 days; aggregate data available
# on S3 via the archives                
DELETE_ARCHIVED_DATA_SQL = """
                            DELETE FROM bus_locations
                            WHERE vehicle_trip_start_date < now()::date - 14;
                            """
                            
COLNAMES = ['vehicle_trip_direction_id', 'vehicle_trip_trip_id',
            'vehicle_trip_start_date', 'start_time', 'end_time', 'runtime']

TARGET_FN = 'daily_performance_summary_%s.csv'

def write_summary_to_s3(cur, storage_conn=s3conn, archive_query=ARCHIVE_DB_SQL,
                        bucket=BUCKET, column_names=COLNAMES, output_filename=TARGET_FN):
    '''
    args:
        cur: psycopg2.extensions.cursor: cursor to postgres database
        storage_conn: boto.s3.connection.S3Connection: connection to S3
        archive_query: str: SQL command to execute for saved summary
        bucket: str: S3 bucket name
        column_names: list: vars to keep for summary
        output_filename: S3 filename
    returns: None
    '''
    D = str(np.datetime64(datetime.now(), 'D') - np.timedelta64(1, 'D'))

    cur.execute(archive_query, (D, D))
    records = cur.fetchall()
    
    # Format to string w. an empty call to .to_csv()
    records_io_string = pd.DataFrame(records, columns=column_names).to_csv()
    
    # Upload to S3
    bucket = storage_conn.get_bucket(bucket, validate=False)
    contents_to_s3(bucket, output_filename%D, records_io_string)

#Archive Data
cur = conn.cursor()
write_summary_to_s3(cur, s3conn)

# Delete Archived Data from DB - See qry desc. above
cur.execute(DELETE_ARCHIVED_DATA_SQL)

cur.close()
conn.close()

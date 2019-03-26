# -*- coding: utf-8 -*-
"""
@author: Dustin.Wilson
RUN LOCAL
File Connects to a AWS hosted Postgres Database and Uploads one of the Main
Reference Tables (service_info). This table contains data on the route (e.g. BX42).
service
"""
import pandas as pd
import psycopg2.extras
from rds_aws_conn import conn
# =============================================================================
# Create Import Trips Files 
TARGET_FILES = ['./Data/Bronx/trips.txt', './Data/Brooklyn/trips.txt', './Data/Manhattan/trips.txt',
                './Data/Queens/trips.txt', './Data/Staten Island/trips.txt']

CREATE_SERVICE_DATA_TBL_SQL = """CREATE TABLE IF NOT EXISTS service_info(
            route_id VARCHAR(7),
            service_id VARCHAR(63),
            trip_id VARCHAR(63),
            trip_headsign VARCHAR(63),
            direction_id INT,
            shape_id VARCHAR(15));"""

cur = conn.cursor()
cur.execute(CREATE_SERVICE_DATA_TBL_SQL)

INSERT_SERVICE_DATA_SQL = """INSERT INTO service_info VALUES %s"""

# Could be done with \copy, elected to do with files in memory as these
# files are small enough...
for fn in TARGET_FILES:
    tmp_df = pd.read_csv(fn)
    trips_seq = tmp_df.to_records(index=False).tolist()
    psycopg2.extras.execute_values(cur, INSERT_SERVICE_DATA_SQL, trips_seq)
    
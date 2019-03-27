# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 01:18:41 2019
Indexes for Main Table of Bus DB
"""
from rds_aws_conn import conn

# Observe ~200x improvement on queries w. WHERE vehicle_trip_trip_id = '####'
# Timed sample query ~ 75s w.o index -> ~0.25s w. index; index creation ~ 7 min
cur = conn.cursor()
cur.execute('''
            CREATE INDEX vttripids ON bus_locations (
                    vehicle_trip_trip_id
            );
            ''')

# Observe ~4x improvement on EOD batch process; revisit
cur.execute('''
            CREATE INDEX vttsx ON bus_locations (
                    vehicle_timestamp, 
                    vehicle_trip_start_date
            );
            ''')

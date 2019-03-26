# -*- coding: utf-8 -*-
"""
@author: Dustin.Wilson
Runs on an EC2 instance and is scheduled to periodically (60s) call the GTFS API,
import data to an RDS DB
"""
import configparser

import re
import psycopg2
import psycopg2.extras

from pandas.io.json import json_normalize

from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import requests

from rds_aws_conn import conn

# GTFS API Auth
PARSER = configparser.ConfigParser()
PARSER.read('./gtfs_api.ini')
GTFS_KEY = PARSER.get('mtaapi', 'GTFS_KEY')

VEHICLE_ENDPOINT = "http://gtfsrt.prod.obanyc.com/vehiclePositions?key"

# Data Processing Functions
def normalize_feed_data(data):
    '''
    Normalizes (flattens) json data to dataframe, for example the json below
    becomes a 3 x 1 pd.DataFrame w. columns 'glossary.chapter', 'glossary.page'
    and 'glossary.title'.

    -- Example ---
    {
        "glossary": {
                "title": "example glossary",
                "page": "35",
                "chapter": "8.1"
        }
    }
    args:
        data: json: raw json data from GTFS api
    returns:
        current_df: pandas.DataFrame:
    '''
    current_df = json_normalize(data['entity'])
    current_df = current_df.rename(columns=lambda x: re.sub(r'\.', '_', x))
    return current_df

def call_gtfs_bus_feed(vehicle_endpoint=VEHICLE_ENDPOINT, key=GTFS_KEY):
    '''
    Requests data from gtfs bus api; unbuffers from protocol
    (see: https://developers.google.com/transit/gtfs-realtime/gtfs-realtime-proto)
    args:
        vehicle_endpoint: str: endpoint to query; Set to endpoint for
        most recent bus locations in this script
        key: str: gtfs developer key
    returns:
        feed_data: json: most recent data from gtfs api w. bus locations
    '''
    response = requests.get('{}={}'.format(vehicle_endpoint, key))
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    feed_data = protobuf_to_dict(feed)
    return feed_data

def prepare_data_seq(feed_data, preserve_columns):
    '''
    Sends data into records format, allows DB to process transaction w.
    `psycopg2.execute_values()` rather than `psycopg2.execute_many()`
    args:
        feed_data: json: raw json data from GTFS api
        preserve_columns: list: columns to retain from normalized (flattened)
    returns:
        r: list: Data as list of records
    '''
    current_df = normalize_feed_data(feed_data)
    current_df = current_df[preserve_columns]
    r = current_df.to_records(index=False).tolist()
    return r

COLNAMES = ['vehicle_trip_direction_id', 'vehicle_trip_trip_id',
            'vehicle_trip_start_date', 'vehicle_timestamp',
            'vehicle_position_latitude', 'vehicle_position_longitude']

# Send most recent location data to RDS - SQL Queries
INSERT_LOCATIONS_SQL = """
                        INSERT INTO bus_locations VALUES %s 
                        ON CONFLICT (vehicle_trip_direction_id,
                                     vehicle_trip_trip_id,
                                     vehicle_timestamp
                                     )
                        DO NOTHING;
                        """

CREATE_LOCATION_TS_SQL = """
                        CREATE TABLE IF NOT EXISTS bus_locations(
                        vehicle_trip_direction_id INT,
                        vehicle_trip_trip_id VARCHAR(255),
                        vehicle_trip_start_date DATE,
                        vehicle_timestamp INT,
                        vehicle_position_latitude NUMERIC(8,5),
                        vehicle_position_longitude NUMERIC(8,5),
                        PRIMARY KEY(vehicle_trip_direction_id,
                                    vehicle_trip_trip_id,
                                    vehicle_timestamp)
                        );
                        """

if __name__ == '__main__':
    # Call API, read and format data...
    current_feed = call_gtfs_bus_feed(VEHICLE_ENDPOINT, GTFS_KEY,)

    cur = conn.cursor()
    cur.execute(CREATE_LOCATION_TS_SQL)

    # Prepare data sequence and insert into PostgreSQL...
    args_sequence = prepare_data_seq(current_feed, preserve_columns=COLNAMES)
    psycopg2.extras.execute_values(cur, INSERT_LOCATIONS_SQL, args_sequence)
    
    # Teardown connection
    cur.close()
    conn.close()
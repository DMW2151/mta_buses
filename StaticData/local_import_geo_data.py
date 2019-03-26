# -*- coding: utf-8 -*-
"""
@author: Dustin.Wilson
Does several geography and mapping related functions.
    * Imports routelines from data download (shapes.txt)
    * Calculates distance between route nodes
    * Saves bus routes and computed distances locally
    * Swaps (lat, lng) -> (lng, lat) and sends to RDS PostGIS for mapping
"""
import math
import pandas as pd
import psycopg2
import psycopg2.extras
from rds_aws_conn import conn
from shapely.geometry import LineString

EARTH_RADIUS = 6371

CREATE_TRIPS_SEQ_TBL_SQL = """
                            CREATE TABLE IF NOT EXISTS shapes_seq(
                            shape_id VARCHAR(63),
                            shp_seq INT,
                            node_lat NUMERIC(8,5),
                            node_lng NUMERIC(8,5),
                            haversine_distance FLOAT,
                            PRIMARY KEY (shape_id, shp_seq));
                            """

INSERT_TRIPS_SEQ_TBL_SQL = """
                            INSERT INTO shapes_seq VALUES %s
                            ON CONFLICT (shape_id, shp_seq) DO NOTHING;
                            """

CREATE_EXT_SQL = """CREATE EXTENSION POSTGIS;"""

CREATE_GEO_TBL_SQL = """
                    CREATE TABLE IF NOT EXISTS bus_routes(
                    shape VARCHAR(63),
                    path GEOGRAPHY
                    );
                    """

# NOTE: (latitude, longitude) is the custom for most calculations. When inserting 
# into postgis the coords are flipped to (longitude, latitude)
TARGET_FILES = ['./Data/Bronx/shapes.txt', './Data/Brooklyn/shapes.txt', 
                './Data/Manhattan/shapes.txt', './Data/Queens/shapes.txt', 
                './Data/Staten Island/shapes.txt']


def parse_ls_wkb(route_linestring):
    '''
    Converts iterable to Shapely geometry (hex-encoded string)
    Args:
        route_linestring: list of tuples, coordinates
    Returns:
        hex-encoded string
    '''
    ls = LineString(route_linestring)
    return ls.wkb_hex

def haversine_distance(st_coord, end_coord): 
    '''
    Calculates Haversine Distance between two coordinates; see calc details
    https://en.wikipedia.org/wiki/Haversine_formula
    Args:
        st_coord: tuple of (lat, lng) in 4326 projection
        end_coord: tuple of (lat, lng) in 4326 projection
    Returns:
        Distance in KMs
    '''
    lat1, lon1 = st_coord; lat2, lon2 = end_coord
    
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    
    A = math.sin(d_lat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2)) *  math.sin(d_lon/2)**2
    C = 2 * math.atan2(math.sqrt(A), math.sqrt(1-A))
    return EARTH_RADIUS * C

# Insert to PG from Local files
shape_file = []
shp_file_ct = dict()

for file in TARGET_FILES:
    with open(file, 'r') as shp_file:
        _ = shp_file.readline() #skip first line
        for line in shp_file:
            shp_id, lat, lng, shp_seq = line.split(',')
            route_ct = shp_file_ct.get(shp_id)
            if route_ct:
                shape_file.append((shp_id, route_ct, float(lat), float(lng)))
            else:
                shape_file.append((shp_id, 0, float(lat), float(lng)))
                shp_file_ct[shp_id] = 0
                
            # increment by one after we observe a route
            shp_file_ct[shp_id] += 1

shape_file_df = pd.DataFrame(shape_file, columns = ['shape_id', 'shp_seq', 
                                                    'node_latitude', 'node_longitude'])

shape_file_df['lat_lagged'] = shape_file_df.groupby(['shape_id'])['node_latitude'].shift(1)
shape_file_df['lng_lagged'] = shape_file_df.groupby(['shape_id'])['node_longitude'].shift(1)

shape_file_df['haversine_distance'] = shape_file_df.apply(lambda rw: 
    haversine_distance((rw.node_latitude, rw.node_longitude), 
                       (rw.lat_lagged, rw.lng_lagged)), 1)

# Create Table & Insert data
cur = conn.cursor()
cur.execute(CREATE_TRIPS_SEQ_TBL_SQL)

# Substitude for df.to_records() as a way to avoid forcing type casts on insert
args_sequence = tuple(tuple(rw) for rw in tuple(shape_file_df[['shape_id', 'shp_seq', 
                      'node_latitude', 'node_longitude', 'haversine_distance']].itertuples(index=False)))
psycopg2.extras.execute_values(cur, INSERT_TRIPS_SEQ_TBL_SQL, args_sequence, page_size=5000)

cur.execute(CREATE_EXT_SQL)
cur.execute(CREATE_GEO_TBL_SQL)

# Swap (lat, lng) -> (lng, lat)
shp_records = {shp_id: [] for shp_id in set(shape_file_df.shape_id)}
for rw in shape_file_df.itertuples():
    shp_records[rw.shape_id].append((rw.node_longitude, rw.node_latitude))

# Coerce to hexcoded linsetring 
for route, linestring in shp_records.items():
    wkbin = parse_ls_wkb(linestring)
    cur.execute(
        'INSERT INTO bus_routes(shape, path)'
        'VALUES (%(shape)s, ST_SetSRID(%(path)s::geometry, %(srid)s))',
        {'shape': route, 'srid': 4326, 'path': wkbin})

cur.close()
conn.close()


    




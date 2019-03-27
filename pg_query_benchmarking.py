# -*- coding: utf-8 -*-
"""
Utility function, creates a cursor to interact with that database (mta_bus) and 
use as a sandbox for testing indexing methods
"""
import os
import configparser
import time
import psycopg2
from psycopg2 import ProgrammingError

def timing_func(func):
    '''
    Defines a decorator to time wrapped function
    Args:
        func: timed function
    Returns:
        result: result of wrapped function
        exec_time: difference of function call and result
    '''
    def wrapper(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        exec_time = te - ts
        return result, exec_time 
    return wrapper

@timing_func
def timed_qry(cur, qry): # Enforce rollback, func shouldn't make changes
    '''
    Wrapper to test query execution
    Args:
        cur: psycopg2.cursor pointing to a database
        qry: SQL query as string
    Returns:
        length of result query, and time to execute (via wrapper)
    '''
    cur.execute(qry)
    try: 
        n_results = len(cur.fetchall())
        return n_results
    except ProgrammingError:
        return 0

# Authentication
db_config = os.path.join('./database.ini')

PARSER = configparser.ConfigParser()
PARSER.read(db_config) #change this to run regardless of the project dir

postgres_section = PARSER.items('postgresql')
PARAMS = dict(postgres_section)

conn = psycopg2.connect(**PARAMS)
conn.autocommit = True

# Testing Query Execution Speed
cur = conn.cursor()

# Timed sample query below (~ 75s w.o index -> ~0.25s w. index)
# Index creation ~ 7 min 16s
timed_qry(cur, '''
              SELECT * FROM bus_locations
              WHERE vehicle_trip_trip_id = '%s';
              '''%('YU_E9-Weekday-SDon-047800_SIM1_11'))



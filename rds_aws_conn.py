# -*- coding: utf-8 -*-
"""
Utility function, creates a cursor to interact with that database (mta_bus)
@author: Dustin.Wilson
"""
import os
import configparser
import psycopg2

def get_main_config(section, option, conf_file='local.conf'):
    """
    Returns the value of an option stored in a section of a config file
    args:
        section: string w. name of a section of a config file
        option: string w. an option contained in a section of a config file
    returns:
        string: value of the target option in the target section
    """
    parser = configparser.ConfigParser()

    cf = os.path.join(os.path.dirname(os.path.realpath(__file__)), conf_file)
    parser.read(cf)
    return parser.get(section, option)


root_proj_dir = get_main_config('paths', 'root')

db_config = os.path.join(root_proj_dir, 'database.ini')

PARSER = configparser.ConfigParser()
PARSER.read(db_config) #change this to run regardless of the project dir

postgres_section = PARSER.items('postgresql')
PARAMS = dict(postgres_section)

conn = psycopg2.connect(**PARAMS)
conn.autocommit = True

# -*- coding: utf-8 -*-
"""
@author: Dustin.Wilson
Upload reference data/non-RDS data to S3 - credential data added to
environ variables at AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as specified 
from config
"""
import os
import configparser
from boto.s3.connection import S3Connection

PARSER = configparser.ConfigParser()
PARSER.read('./s3_auth.ini')

# Set Environ Vars. Gets default bucket from config file
os.environ['AWS_ACCESS_KEY_ID'] = PARSER.get('s3', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = PARSER.get('s3', 'AWS_SECRET_ACCESS_KEY')
BUCKET = PARSER.get('s3', 'BUCKET')

def contents_to_s3(bucket, filename, contents):
    K = bucket.new_key(filename)
    K.set_contents_from_string(contents)
                
s3conn = S3Connection()


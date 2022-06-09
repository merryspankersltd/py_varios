# -*- coding: utf-8 -*-

""" BAAS importer

Merry Spankers Ltd
the unlicence 2022

https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2020/
This module will download multiple csv files from data.gouv.fr
and load them into a preconfigured postgres schema

inputs:
    - postgres connexion string
    - csv url list
outputs:
    - populated schema with new tables

code inspiration: https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table
"""

# GLOBALS
INI_FILE = "params.ini"
CONN_FILE = "postgres_credentials.ini"

import os
import io
import chardet
import configparser
import requests
import pandas as pd
from sqlalchemy import create_engine

def get_csvs(dataset_id):
    """
    retrieve csvs as pseudofiles from data.gouv.fr API
    input: dataset_id as string
    returns: list of dicts (url, name, cvsIO)"""
    
    # retrieve csv urls
    res = requests.get('https://www.data.gouv.fr/api/1/datasets/' + dataset_id)
    csvs = [
        {'url': r['url'], 'name': os.path.splitext(r['title'])[0]}
        for r in res.json()['resources'] if r['format']=='csv']

    return csvs

def inject(csv, separator, connection, dest_schema):
    """
    injects csv into postgres schema"""
    
    # use pandas as csv dl/reader (on error try another separator)
    print('will read ' + csv['name'])
    df = pd.read_csv(csv['url'], sep=separator, encoding='ISO-8859-1')
    df.columns = [c.lower() for c in df.columns]

    # use sql_alchemy as postgres writer
    engine = create_engine(connection)
    df.to_sql(os.path.splitext(csv['name'])[0], engine, schema=dest_schema)

if __name__ == "__main__":

    # get postgres credentials and build connection string
    conn = configparser.ConfigParser()
    conn.read(CONN_FILE)
    Host = conn['server']['Host']
    Port = conn['server']['Port']
    Base = conn['server']['Base']
    user = conn['postgres']['user']
    password = conn['postgres']['password']
    
    # get io params from ini
    config = configparser.ConfigParser()
    config.read(INI_FILE)
    dataset_id = config['BAAS']['DataGouvID']
    dest_schema = config['BAAS']['Schema']
    
    # build connection string
    conn_str = 'postgresql://' + user + ':' + password + '@' + Host + ':' + Port + '/' + Base

    # get csv list
    csvs = get_csvs(dataset_id)

    # cycle through csv and inject them into
    for csv in csvs:
        # set separator (project specific)
        if 'vehicules-immatricules-baac' in csv['name']:
            separator = ';'
        elif '2020' in csv['name'] or '2019' in csv['name']:
            separator = ';'
        else:
            separator = ','
        inject(csv, separator, conn_str, dest_schema)
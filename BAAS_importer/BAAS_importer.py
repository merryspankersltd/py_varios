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

# GLOBALS : ini files
INI_FILE = "params.ini"
CONN_FILE = "postgres_credentials.ini"

import os
import configparser
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect

def get_conn_params(path_to_ini):
    '''reads ini files
    returns Host, Port, Base, user, password'''

    conn = configparser.ConfigParser()
    conn.read(path_to_ini)
    Host = conn['server']['Host']
    Port = conn['server']['Port']
    Base = conn['server']['Base']
    user = conn['postgres']['user']
    password = conn['postgres']['password']

    return Host, Port, Base, user, password

def get_ini_params(path_to_ini):
    '''reads ini file
    returns dataset_id, dest_schema'''

    config = configparser.ConfigParser()
    config.read(path_to_ini)
    dataset_id = config['BAAS']['DataGouvID']
    dest_schema = config['BAAS']['Schema']

    return dataset_id, dest_schema

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

def inject(csv, separator, encoding, connection, dest_schema):
    """
    injects csv into postgres schema"""
    
    # establish connection to postgres and get list of existing tables
    engine = create_engine(connection)
    inspector = inspect(engine)
    existing_tbls = inspector.get_table_names(schema=dest_schema)

    # use pandas as csv dl/reader (on error try another separator)
    # do nothing if table already exists
    if not csv['name'] in existing_tbls:
        # pd.read_csv(url) magic !!!
        df = pd.read_csv(csv['url'], sep=separator, encoding=encoding)
        df.columns = [c.lower() for c in df.columns]
        # use sql_alchemy as postgres writer : df.to_sql(psql) magic !!!
        df.to_sql(os.path.splitext(csv['name'])[0], engine, schema=dest_schema)
        print(csv['name'] + ' injected')
    else:
        print(csv['name'] + ' already exists: not injected')

if __name__ == "__main__":

    # get postgres credentials and io params from ini files
    Host, Port, Base, user, password = get_conn_params(CONN_FILE)
    dataset_id, dest_schema = get_ini_params(INI_FILE)
    
    # build connection string to postgres destination
    conn_str = 'postgresql://' + user + ':' + password + '@' + Host + ':' + Port + '/' + Base

    # get csv list from data.gouv.fr API
    csvs = get_csvs(dataset_id)

    # cycle through csv and inject them into postgres destination
    for csv in csvs:
        # set separator and encoding (project specific)
        if 'vehicules-immatricules-baac' in csv['name']:
            separator = ';'
            encoding = 'utf-8'
        elif any([y in csv['name'] for y in ['2019', '2020']]):
            separator = ';'
            encoding = 'utf-8'
        elif any([y in csv['name'] for y in ['2009', '2008', '2007', '2006', '2005']]):
            separator = '\t'
            encoding ='iso-8859-1' # passable...
        else: # 2018 -> 2010
            separator = ';'
            encoding = 'iso-8859-1'
        # inject
        inject(csv, separator, encoding, conn_str, dest_schema)
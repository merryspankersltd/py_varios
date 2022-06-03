# -*- coding: utf-8 -*-

""" BAAS importer

Merry Spankers Ltd 2022

https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2020/
This module will download multiple csv files from data.gouv.fr
and load them into a preconfigured postgres schema

inputs:
-------
    - postgres connexion string
    - csv url list
outputs:
--------
    - populated schema with new tables

code inspiration: https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table
"""

# GLOBALS
INI_FILE = "connexion_string.ini"

import os
import io
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
        {'url': r['url'], 'name': os.spslitext(r['title'])[0]}
        for r in res.json()['resources'] if r['format']=='csv']
    
    # dl csvs as StringIO
    for csv in csvs:
        res = requests.get(csv['url'])
        csvIO = io.StringIO()
        csvIO.write(res.content)
        csvIO.seek(0)
        res['csv'] = csvIO

    return csvs

def inject(csv, connection, dest_schema):
    """
    injects csv into postgres schema"""
    
    # use pandas as csv reader
    df = pd.read_csv(csv)
    df.columns = [c.lower() for c in df.columns]

    # use sql_alchemy as postgres writer
    engine = create_engine(connection)
    df.to_sql(os.path.splitext(os.path.basename(csv))[0], engine, schema=dest_schema)

if __name__ == "__main__":

    # get params from ini file
    config = configparser.ConfigParser()
    config.read(INI_FILE)
    connection = config['DEFAULT']['ConnectionString']
    dataset_id = config['BAAS']['DataGouvID']
    dest_schema = config['BAAS']['Schema']

    # get csv list
    csvs = get_csvs(dataset_id)
    
    # cycle through csv and inject them into
    for csv in csvs:
        inject(csv)
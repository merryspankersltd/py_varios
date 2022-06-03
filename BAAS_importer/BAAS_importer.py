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
import configparser
import pandas as pd
from sqlalchemy import create_engine

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
    dest_schema = config['BAAS']['Schema']

    # get csv list
    
    with open(CSV_URLS, 'r') as f:
        csvs = [csv.rstrip() for csv in f.readlines()]

    # cycle through csv and inject them into
    for csv in csvs:
        inject(csv)
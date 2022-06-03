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
CONN = "connexion_string.ini"
CSV_URLS = "csv_list.txt"

import pandas as pd
from sqlalchemy import create_engine

# get csv list
with open(CSV_URLS, 'r') as f:
    csvs = f.readlines()
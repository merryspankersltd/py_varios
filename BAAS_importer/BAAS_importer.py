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
"""

# GLOBALS
CONN = None
CSV_URLS = "csv_list.txt"


import pandas as pd
from sqlalchemy import create_engine
import requests
from io import BytesIO
from zipfile import ZipFile
import configparser


# GLOBALS : ini files
INI_FILE = "params.ini"
CONN_FILE = "postgres_credentials.ini"

urls = [
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_69D.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_200046977.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_74.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_73.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_63.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_43.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_42.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_38.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_26.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_15.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_07.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_03.zip', 
    'https://www.orcae-auvergne-rhone-alpes.fr/fileadmin/user_upload/mediatheque/orcae/Tableaux_donnees/Donnees/donnees_detaillees_communes_01.zip'
]

def get_conn_params(path_to_ini):
    '''reads ini files
    returns Host, Port, Base, user, password'''

    conn = configparser.ConfigParser()
    conn.read(path_to_ini)
    Host = conn['server']['Host']
    Port = conn['server']['Port']
    Base = conn['server']['Base']
    user = conn['user 1']['user']
    password = conn['user 1']['password']

    return Host, Port, Base, user, password

def url_to_dfs(url, targets):
    '''
    downloads a zip, extract multiple specific csvs and returns a list of panda dataframes
    targets is a list of filename patterns to be found in the zipfile'''
    
    # download
    res = requests.get(url)
    # read zip from response
    azip = ZipFile(BytesIO(res.content))
    # loads into a pandas dataframe
    dfs = [
        pd.read_csv(
            BytesIO(azip.read(csv_name)),
            sep=';', encoding='cp1252', decimal=',',
            na_values='S', dtype={'code insee': str})
        for csv_name in azip.namelist()
        if any([target[0] in csv_name for target in targets])]

    return dfs

def inject_df(df, engine, value_field, dest_schema):

    filtered_df = df[(
        df["secteur"].isin(['Branche énergie', 'Tous secteurs hors branche énergie'])
        & (df["énergie"] == 'Toutes énergies')
        & (df["usage"] == 'Tous usages'))]
    #   group by and sums by depcom
    parcom = filtered_df[["code insee", "année", value_field]].groupby(["code insee", "année"], as_index=False).sum()
    #   pivot by years
    pivoted = parcom.pivot(index="code insee", columns="année", values=value_field)
    #   establish connection to psql and injects
    pivoted.to_sql(value_field, engine, dest_schema)

if __name__ == "__main__":

    # retrieve data from urls and filename patterns :
    # %%
    #   targets are filename patterns to be found in a zip archive
    targets = [
        ('orcae_eges_communes_', 'valeur (kteqCO2)', 'orcae_conso_CO2'),
        ('orcae_conso_communes_', 'valeur (GWh)', 'orcae_conso_Energie')]
    # %%
    #   dfs is a list of lists of dataframes
    dfs = [url_to_dfs(url, targets) for url in urls]

    # pandas operations :
    # %%
    #   concatenates dataframes by theme (cf targets)
    dfs_concat = [pd.concat(zipped_df) for zipped_df in list(zip(*dfs))]
    # %%
    # establisch connection with server
    Host, Port, Base, user, password = get_conn_params(CONN_FILE)
    engine = create_engine('postgresql://' + user + ':' + password + '@' + Host + ':' + Port + '/' + Base)
    # %%
    # injects concatenated dfs
    for target, df in list(zip(targets, dfs_concat)):
        inject_df(df, engine, target[1], target[2])
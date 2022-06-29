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
    user = conn['postgres']['user']
    password = conn['postgres']['password']

    return Host, Port, Base, user, password

def url_to_df(url):
    '''
    downloads a zip, extract one specific csv and returns a panda dataframe'''
    
    # download
    res = requests.get(url)
    # read zip from response
    azip = ZipFile(BytesIO(res.content))
    # gets specific csv name based on pattern
    csv_name = [n for n in azip.namelist if 'donnees_detaillees_communes_' in n][0]
    # extracts csv to BytesIO file
    csv = azip.read(csv_name)
    # loads into a pandas dataframe
    df = pd.read_csv(BytesIO(csv), sep=';', encoding='cp1252', decimal=',', na_values='S', dtype={'code insee': str})

    return df

if __name__ == "__main__":

    # builds a list of dataframes from urls
    dfs = [url_to_df(url) for url in urls]

    # pandas operations :
    #   concatenates all dataframes into one (departements -> region)
    df_concat = pd.concat(dfs)
    #   filters rows (rows cumulates items and sums of items: keep sums only)
    conso = df_concat[(
        df_concat["secteur"].isin(['Branche énergie', 'Tous secteurs hors branche énergie'])
        & (df_concat["énergie"] == 'Toutes énergies')
        & (df_concat["usage"] == 'Tous usages'))]
    #   group by and sums by depcom
    parcom = conso[["code insee", "année", "valeur (kteqCO2)"]].groupby(["code insee", "année"], as_index=False).sum()
    #   pivot by years
    pivoted = parcom.pivot(index="code insee", columns="année", values="valeur (kteqCO2)")
    #   establish connection to psql and injects
    Host, Port, Base, user, password = get_conn_params(CONN_FILE)
    engine = create_engine('postgresql://' + user + ':' + password + '@' + Host + ':' + Port + '/' + Base)
    pivoted.to_sql('orcae_consoCO2', engine, 'd_mobilite')

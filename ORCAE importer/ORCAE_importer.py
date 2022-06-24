csvs = [
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_01_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_03_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_07_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_15_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_200046977_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_26_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_38_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_42_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_43_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_63_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_69D_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_73_2022-06-09.csv',
    'C:\\Users\\marcl\\Documents\\pro\\data\\orcae\\eges_communes\\orcae_eges_communes_74_2022-06-09.csv'
    ]

df = pd.concat([pd.read_csv(csv, sep=';', encoding='cp1252', decimal=',', na_values='S', dtype={'code insee': str}) for csv in csvs])
conso = df[(df["secteur"].isin(['Branche énergie', 'Tous secteurs hors branche énergie']) & (df["énergie"] == 'Toutes énergies') & (df["usage"] == 'Tous usages'))]
parcom = conso[["code insee", "année", "valeur (kteqCO2)"]].groupby(["code insee", "année"], as_index=False).sum()
pivoted = parcom.pivot(index="code insee", columns="année", values="valeur (kteqCO2)")
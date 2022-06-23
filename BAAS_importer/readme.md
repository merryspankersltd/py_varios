# Bases de données annuelles des accidents corporels de la circulation routière

source des données: https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2020/

## La source de données

D'après data.gouv.fr: 

> Pour chaque accident corporel (soit un accident survenu sur une voie ouverte à la circulation publique, impliquant au moins un véhicule et ayant fait au moins une victime ayant nécessité des soins), des saisies d’information décrivant l’accident sont effectuées par l’unité des forces de l’ordre (police, gendarmerie, etc.) qui est intervenue sur le lieu de l’accident. Ces saisies sont rassemblées dans une fiche intitulée bulletin d’analyse des accidents corporels. L’ensemble de ces fiches constitue le fichier national des accidents corporels de la circulation dit « Fichier BAAC » administré par l’Observatoire national interministériel de la sécurité routière "ONISR".

A noter qu'un certain nombre d'indicateurs ont été labellisés par l'onisr: https://www.onisr.securite-routiere.gouv.fr/outils-statistiques/indicateurs-labellises

## import des sources

Classique: au moyen d'un script exploitant l'API data.gouv:

1. récupération de la liste des csv du dépôt
2. injection des csv dans un schéma postgres

Le code magique à l'aide de pandas et de sqlalchemy:

```python
df = pd.read_csv(csv)
df.to_sql(csv, engine, schema)
```
où engine est une connexion à une base postgres et schema la destination.

Attention:
- la qualité des fichiers est très variable (géocodage aléatoire)
- les standards varient d'une année ou d'un fichier à l'autre (encodage, sparateur) ce qui a obligé à mettre en place une série de conditions pour traiter les cas particuliers
- documentation pas à jour, fragmentaire

Au final, les fichiers sont importés tels quels, ils serviront de base à la création des vues.

## le modèle de données

### données principales

4 tables par millésime:
- caractéristiques (porte la géolocalisation)
- lieux
- usagers
- véhicules

Caractéristiques + lieux forment l'objet accident (identifiant unique d'accident). A 1 accident correspond 1 ou plusieurs véhicules (identifiant de véhicule) et à chaque véhicule 1 ou plusieurs usagers.

Les piétons impliqués apparaissent comme "passagers" du véhicule qui les a pris à partie mais portent l'information de piéton dans le champ place à bord.

### données complémentaires

- des tables de détails sur les véhicules impliqués
- des bases de données agrégées 2005 - 2010 et 2006 2011

Ces sources ne sont pas documentées, elles sont confuses, et pour ces raisons elles n'ont pas été ajoutées au modèle de données.

A noter enfin que les modèles de données changent d'un millésimme l'autre, ces changements sont partiellement documentés dans des pdf partagés dans le dépôt.

### contrôle qualité

Le contrôle du géocodage fait apparaître de grosses lacunes jusqu'à 2018: on voit apparaître une bande complètement blanche depuis la Normandie jusqu'au Pays Basque. Les points correspondant apparaissent disséminés dans l'Atlantique mais respectent leur latitude initiale (latitude juste, référentiel longitudinal erronné).
En dehors de cette zone, rien ne permet de penser qu'il existe des lacunes, mais il est fort probable que des points appartenant à la bande disparue puissent se retrouver n'importe où sur le territoire.

A partir de 2019 le géocodage semble cohérent.

## Nettoyage et constitution des vues

A chaque table source correspond une vue raffinée corrigeant le géocodage (paramètres de correction adaptés à chaque table) et nettoyant les champs (nommages, libellés), l'objectif étant d'harmoniser le plus possibles les vues résultant d'un millésimme à l'autre:

- BAAS_caracteristiques_20xx
- BAAS_lieux_20xx
- BAAS_usagers_20xx
- BAAS_vehicules_20xx

A partir de ce premier niveau de vues, une série de vues complètes (intégrant les champs caractéristiques, lieux et véhicules) sont produites par millésime, ces vues sont agrégées à la commune afin d'obtenir des comptages communaux (victimes par niveaux de gravité, les usagers indemnes sont exclus de cette vue)

- BAAS_victimes_20xx
- BAAS_victimes_com_20xx

Enfin une table de synthèse de toutes les victimes de 2010 à 2020 est constituée afin de rechercher les points noirs sur le temps long:

- BAA_victimes_2010_2020



# Py varios
Uncurated python scripts, various purposes, mostly for personal use...
## BAAS importer
Importation de tous les csv du repo BAAS de data.gouv.fr dans un schema postgresql.
BAAS est la base de données d'accidentologie routière française, ell est alimentée par les rapports de police: https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2020/
Le script accède à l'API de dataza.gouv.fr pour obtenir une liste des csv du repo, télécharge les fichiers et les injecte dans un schéma postgresql avec credentials.
Fichiers inclus:
- BAAS_importer.py: le script
- params.ini: identification du repo par son ID et désignation d'un schema en output
- postgres_credentials_template.ini: user/pass de l'utilisateur postgres qui écrit dans le schéma. Ce fichier doit être modifié avec un user/pass valide pour enregistré sous postgres_credentials.ini dans le même dossier que BAAS_importer.ini

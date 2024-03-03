# Datalake : Avis twitter sur les jeux videos 

Projet de création d'un datatlake sur le thème des jeux vidéos.<br>
* Phase 1 : Récupérer une liste de jeux (dataset de jeux avec dates de sorties et évaluation || ``Kaggle``)
* Phase 2 : Collecter en batch les avis twitter sur la base des noms des jeux : ``API Twitter``.
* Phase 3 : Requêter des stats basiques sur la data collectée avec ``pySpark`` (mode local - format ``parquet``)
* Phase 4 : Upload la <b>data en batch</b> sur un noeud ``Elastic Search`` + dashboard ``Kibana``.  

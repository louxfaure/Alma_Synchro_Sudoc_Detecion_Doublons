# Script de suivi et de fusion des doublons du circuit de synchronisation des notices électroniques dans le SUDOC

Ce script intervient dans [la procédure de fusion des notices de documents électroniques](https://rebub.u-bordeaux.fr/index.php/wiki/configuration/alma/gestion-des-ressources/identifier-et-corriger-les-doublons-de-notices-electroniques-dans-le-cadre-du-circuit-de-synchronisation-sudoc/). Il analyse quotidiennement le rapport de traitement du chargeur **Import Notices Abes (Marc21)** pour extraire les notices en multi-match( plusieurs notices avec le même PPN dans Alma) et fournir un rapport préformaté permettant à un opérateur de lancer le traitement Alma **"Fusionner les notices et combiner l'inventaire"**. Le script permet aussi de réaliser l'export des notices multimatch toujours présentes dans la CZ pour permettare la copie de ces dernières dans la zone réseau. Cette opération est un préalable nécessaire à la fusion des notices. 

## Détail schématique  des traitements

1. Récupère le rapport de traitement et extrait la liste des multimatch (ppn + liste des mms id)
2. Pour chaque groupe :
  - Exclue les notices au format Unimarc. Ces cas sont signalés dans le rapport "liste_des_anomalies_sur_format"
  - Repère les notices de la CZ. Ces cas sont signalés dans la liste "liste_pour_reloc_NZ"
  - Détermine la notice préférée. Notice dont le système d'porigine est "ABES" ou la dernière notice du groupe traitée.
3. Formate les données pour les écrire dans un fichier "notices_a_fusionner.csv". Crée un fichier pour 100 groupes. C'est la limite imposée par le traitement.
4. Crée un ensemble avec la liste "liste_pour_reloc_NZ"
5. Lance un export des notices de l'ensemble via un job Alma. Les notices sont exportés vers un serveur ftp.
6. Envoie les rapports de traitement par mail aux opérateurs

## Traitements externes au script
1. Les notices exportées sont réimportées dans Alma pour les copier de la CZ vers la NZ. Le job d'import "Copie local des Notices CZ" s'éxécute automatiquement. Il est configuré pour s'exécuter après le passage du script. 
2. S'il y a lieu, l'opérateur exécute le job "Fusionner les notices et combiner l'inventaire".

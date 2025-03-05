#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import math
import logging
import csv
import xml.etree.ElementTree as ET
from datetime import datetime,date
from os import listdir, path, getenv, remove, mkdir
#Modules maison 
from Services import logs,mail,Alma_api_imports, AlmaRecord, AlmaSet,AlmaJob

SERVICE = "Alma_Fusion_Notices_Docelec"
API_KEY = getenv('PROD_NETWORK_BIB_API')
REP = '/tmp/Notices_a_fusionner/'
LOGS_LEVEL = 'DEBUG'
LOGS_DIR = getenv('LOGS_PATH')
JOB_ID = 'S10796120430004671' # Plannifié
# JOB_ID = 'S11850852720004671' # Manuel
EXPORT_JOB_PAREMETERS_FILE = './Conf/export_job_parameters.xml'
JOB_IMPORT_ID = 'S15731461420004671'

today = date.today()
now = datetime.now()
group_id = 1
num_fichier = 1
liste_de_cent_groupe = []


#On initialise le logger
logs.setup_logging(name=SERVICE, level=LOGS_LEVEL,log_dir=LOGS_DIR)
log_module = logging.getLogger(SERVICE)
log_module.info("Début du traitement")
log_module.debug(today)


# Liste pour signaler les anomalies
rapports = {
    'liste_des_anomalies_sur_format' : {
	    'liste' : [],
	    'column': ['PPN','MMSID'],
    },
    'liste_pour_reloc_NZ' : {
        'liste' : [],
        'column': ['MMS ID'],
    },
    'liste_des_anomalies_api' : {
        'liste' : [],
        'column':['PPN','Message erreur'],
    },
    'liste_des_id_sans_doublon' : {
        'liste' : [],
        'column': ['PPN'],
    }
}


def rechercher_notice_preferee(liste_mmms_ids,ppn,nb_notices_dans_groupe) :
    """Dans une liste de notices doublons (ayant le même identifiant) :
    - Identifie la notice à préférer (notice local et abes)
    - Identifie les notices CZ à copier dans la NZ
    - Exclue les notices Unimarc

    Args:
        liste_mmms_ids (array): liste de mms id
        ppn (string): ppn
        nb_notices_dans_groupe (integer): nombre de notices dans le groupe

    Returns:
        array: liste des données formatées pour lancer le service Fusionnées les notices et combiner l'inventaire
    """
    statut = "Pas d'anomalie"
    liste_des_membres_du_groupe = []
    notice_pref_dans_groupe = False
    log_module.debug("preferred_record_definition")
    index = 0
    # Pour chaquue mms_id du grpupe
    for mms_id in liste_mmms_ids :
        # log_module.debug("\t-->{}".format(mms_id))
        doc = AlmaRecord.AlmaRecord(mms_id, apikey=API_KEY)
        log_module.debug(doc.error_status)
        # Si j'ai une notice Unimarc dans le groupe alors j'exclue la notice du traitement de fusion
        if doc.error_status :
            rapports['liste_des_anomalies_api']['liste'].append([mms_id,doc.error_message])
            log_module.debug(doc.error_message)
            nb_notices_dans_groupe -= 1
            continue
        if doc.is_unimarc_record() :
            rapports['liste_des_anomalies_sur_format']['liste'].append([mms_id,ppn])
            nb_notices_dans_groupe -= 1
            continue
        # Si c'est une notice de la CZ :
        index += 1
        if doc.is_cz_record() :
            # On récupère le MMSId pour faire redescendre la notice dans la NZ
            rapports['liste_pour_reloc_NZ']['liste'].append(mms_id)
        # Si j'ai déjà défini une notice préférée dans le groupe, je passe toutes les autres à merge
        if notice_pref_dans_groupe :
            operation = "merge"
        # Si je n'ai pas défini de notices préférés et que je suis sur ma dernière notice du groupe je la considère comme notice préférée
        elif index == nb_notices_dans_groupe :
            operation = "preferred"
       # Sinon je teste le système d'origine si celui-ci = ABES ou contient SUDOC alors je considère que c'est ma notice péférée
        elif doc.is_abes_record():
            operation = "preferred"
            notice_pref_dans_groupe = True
        else :
            operation = "merge"
        membre_du_groupe = {
            'Group Number':'',
            'MMSID' : mms_id,
            'Identifier' : ppn,
            'Records In Group' : nb_notices_dans_groupe,
            'Operation' : operation,
            'Material Type' : 'Book',
            'Brief Level' : doc.brief_level(),
            'Resource Type': 'Book - Electronic',
            'Held By': '',
            'Title' : doc.titre()
        } 
        liste_des_membres_du_groupe.append(membre_du_groupe)
    return nb_notices_dans_groupe, liste_des_membres_du_groupe

def obtenir_job_export_parameters(set_id):
    """Renseigne les paramètres (identifiant de l'ensemble et nom du job) du job d'exportation des données 

    Args:
        set_id (string): identifiant de l'ensemble contenant les notices à exporter
    """
    # Récupère les paramètres
    tree = ET.parse(EXPORT_JOB_PAREMETERS_FILE)
    root = tree.getroot()
    # Trouver le paramètre "set_id" et mettre à jour sa valeur
    for parameter in root.findall(".//parameter"):
        if parameter.find("name").text == "set_id":
            parameter.find("value").text = set_id
        if parameter.find("name").text == "job_name":
            parameter.find("value").text = "Synchro SUDOC doc Elec : export des notices CZ pour redescente dans la NZ -- {}".format(today)
    # Convertir l'arborescence XML modifiée en texte
    return ET.tostring(root, encoding='utf-8').decode('utf-8')

def rediger_envoyer_message(sur_erreur, sujet, text, job_info=""):
    if job_info == "" :
        job_info_text = ""
    else :
        job_info_text = "\r\nInformations issues du rapport de chargement pour le traitement n° {} :\r\n\t-Nombre de notices taitées = {}\r\n\t-Nombre notices importées = {}\r\n\t-Nombre de notices en multi-match = {}\r\n\t-Nombre de notices non chargées = {}\r\n Rapport de traitement des MultiMatchs : r\n\t- Nombre de portfolios liés à des notices Unimarc = {}\n\t- Nombre de notices pour laqueles l'API a renvoé une erreur = {}\n\t- Nombre de notices sans doublon = {}\n\t- Nombre de notices à réimporter = {}".format(
            job_info["job_id"],
            job_info["nb_notices_fournies"],
            job_info["nb_notices_chargees"],
            job_info["nb_multi-match"],
            job_info["nb_notices_non_chargees"],
            len(rapports['liste_des_anomalies_sur_format']['liste']),
            len(rapports['liste_des_anomalies_api']['liste']),
            len(rapports['liste_des_id_sans_doublon']['liste']),
            len(rapports['liste_pour_reloc_NZ']['liste'])
        )
    text_erreur = "Bonjour,{}\r\nUne erreur est survenue au cours de l'exécution du job de traitement.Voici le message d'erreur : \r\n{}.\r\nVeuilllez consulter les logs du traitement.\r\nCordialement,"
    text_succes = "Bonjour,{}\r\n{}\r\nCordialement,"
    sujet_message = "{service} - {date} : {statut} : {sujet}".format(
        service=SERVICE,
        date=today,
        statut="Erreur" if sur_erreur else "Succés",
        sujet=sujet 
    )
    fichiers=[path.join(REP, f) for f in listdir(REP) if path.isfile(path.join(REP, f))]
    texte_message = text_erreur.format(job_info_text,text) if sur_erreur else text_succes.format(job_info_text,text)
    message = mail.Mail()
    message.envoie(mailfrom=getenv('MAILFROM'),mailto=getenv('MAILTO'),subject=sujet_message,text=texte_message,fichiers=fichiers)

# crétaion et vidage du répertoire de stockage temporaire des fichiers
if not path.exists(REP):
    mkdir(REP)
else :
    for f in listdir(REP):
        remove(path.join(REP, f))
log_module.info("Vidage du répertoire : {}".format(REP))

# Récupération de la liste des instance du job d'import S10796120430004671 exécutée ce jour
job_import = Alma_api_imports.AlmaJob_Instance_Id(JOB_ID,today,today,apikey=API_KEY,service=SERVICE)
# log_module.debug(job_import)
if job_import.error_status :
    rediger_envoyer_message(True, "Impossible de récupérer la listes des traitements d'import Marc21 du jour", job_import.error_message)
    log_module.error(job_import.error_message)
    log_module.info("FIN DU TRAITEMENT")
    exit()
if job_import.get_nb_de_jobs()== 0 :
    rediger_envoyer_message(False, "Impossible de récupérer la listes des traitements d'import Marc21 du jour", "Bonjour,\r\nIl n'y a pas eu de chargements de notices SUDOC Marc21 ce jour.\r\nLe traitement a rrété son exécution.")
    log_module.info("FIN DU TRAITEMENT : Pas de traitement d'import ce jour")
    exit()
# Récupération de l'ID du job exécuté ce jour
job_instance_id = job_import.job_instance_id 
log_module.debug(job_instance_id)
# Récupération des informations du rapport de traitement
job_infos =job_import.get_job_infos()
if job_infos['est_erreur'] :
    rediger_envoyer_message(True, "Impossible de récupérer le rapport du traitement d'import des notices ABES Marc21", job_infos['msg_erreur'])
    log_module.error(job_infos['msg_erreur'])
    log_module.info("FIN DU TRAITEMENT : impossible de récupérer le rapport du traitement d'import des notices ABES Marc21")
    exit()
# log_module.debug(job_infos)


# Récupération de la liste des multimatchs
#On évalue le nombre d'appel nécessaires pour obtenir la liste
nb_appels = math.ceil(job_infos['nb_multi-match']/100)
# log_module.debug(nb_appels)

liste_des_multimatchs = []
for i in range(nb_appels):
    offset = i*100
    is_error, reponse = job_import.get_multimatch(offset=offset,limit=100)
    if is_error :
        rediger_envoyer_message(True, "Impossible de récupérer la liste des MultiMatchs", reponse,job_info=job_infos)
        log_module.error(reponse)
        log_module.info("Impossible de récupérer la liste des multimatchs FIN DU TRAITEMENT")
        exit()
    liste_des_multimatchs.extend(reponse)


# log_module.debug(len(liste_des_multimatchs))
if len(liste_des_multimatchs)==0:
    rediger_envoyer_message(False, "Pas de Multimatch ce sour ", "Il n'y a pas de cas de multimatch ce jour.\r\nLe traitement a interrompu son exécution.",job_info=job_infos)
    log_module.info("Pas de multimatchs : FIN DU TRAITEMENT")
    exit()
for dict in liste_des_multimatchs :
    log_module.debug(dict['ppn'])
    nb_notices_dans_groupe, liste_des_membres_du_groupe = rechercher_notice_preferee(dict['mms_ids'],dict['ppn'],len(dict['mms_ids']))
    log_module.debug(liste_des_membres_du_groupe)
    # Si j'ai exclu une ou plusieur notices du groupe alors je regarde s'il me reste plus d'une notice dans le groupe. 
    if nb_notices_dans_groupe <= 1 :
        rapports['liste_des_id_sans_doublon']['liste'].append([dict['ppn']])
        continue
    for dict in liste_des_membres_du_groupe :
         dict['Group Number'] = group_id
    liste_de_cent_groupe.extend(liste_des_membres_du_groupe)
    group_id += 1
    #Si j'ai traité 100 groupes, je créé un fichier avec mes candidats à la fusion. Le traitement ne traite que des lots de 100 groupes
    if group_id%100 == 0 :
        with open('{}notices_a_fusionner_{}.csv'.format(REP,num_fichier), 'w') as f:
            mywriter = csv.DictWriter(f, fieldnames=liste_de_cent_groupe[0].keys(),delimiter=',')
            mywriter.writeheader()
            mywriter.writerows(liste_de_cent_groupe)
            liste_de_cent_groupe = []
            num_fichier +=1    
#  On envoie les dernières notices dans un fichier si il reste des groupes à fusioner
if len(liste_de_cent_groupe) > 1 :
    with open('{}notices_a_fusionner_{}.csv'.format(REP,num_fichier), 'w') as f:
        mywriter = csv.DictWriter(f, fieldnames=liste_de_cent_groupe[0].keys(),delimiter=',')
        mywriter.writeheader()
        mywriter.writerows(liste_de_cent_groupe)

    
# Rédaction des autres rapports
for file_name, rapport in rapports.items() :
    log_module.info("{}:{}".format(file_name,len(rapport['liste'])))
    if len(rapport['liste']) > 0 :
        # ajout des entêtes de colonnes
        rapport['liste'].insert(0,rapport['column'])
        with open('{}{}.csv'.format(REP,file_name), 'w') as f:
            mywriter = csv.writer(f, delimiter=',')
            if file_name == 'liste_pour_reloc_NZ' :
                mywriter.writerows(zip(rapport['liste']))
            else :
                mywriter.writerows(rapport['liste'])
        # Suppression des entêtes de colonnes
        rapport['liste'].pop(0)


# Si pas de notices à faire redescendre dans la NZ on arrête là
if len(rapports['liste_pour_reloc_NZ']['liste']) == 0 :
    rediger_envoyer_message(False, "Fin du traitement - Pas de notice à recharger", "Il n'y a pas de notice à faire redescendre dans la NZ.\r\nAttention. Il y a peut être des notices à fusionner.",job_info=job_infos)
    log_module.info("Pas de multimatchs : FIN DU TRAITEMENT")
    exit()

# Rechargement des notices de la CZ  pour les faire redescendre dans la NZ
# Création du SET
set = AlmaSet.AlmaSet(create=True,nom="Detection des doublons - {}".format(now),apikey=API_KEY, service=SERVICE)
if set.error_status :
    rediger_envoyer_message(True, "Impossible de créer le jeu pour l'export des notices", set.error_message,job_info=job_infos)
    log_module.error(set.error_message)
    log_module.info("FIN DU TRAITEMENT : impossible de créer le jeu pour l'export des notices")
    exit()
# Envoie notices dans le set
# L'API ne prend que 1000 mmsid par envoie si ma liste contient plus de 1000 mmsid alors je scinde ma  liste en liste de 1000

liste_pour_reloc_NZ = rapports['liste_pour_reloc_NZ']['liste']
nb_notices_a_recharger = len(liste_pour_reloc_NZ)

if nb_notices_a_recharger <= 1000:
    set.add_members(liste_pour_reloc_NZ,accept='json')
    if set.error_status :
        rediger_envoyer_message(True, "Impossible d'ajouter  less MMSID au jeu pour l'export des notices", set.error_message,job_info=job_infos)
        log_module.error(set.error_message)
        log_module.info("FIN DU TRAITEMENT : impossible de créer le jeu pour l'export des notices")
        exit()
# Envoie notice 
else :
    liste_de_lites = [liste_pour_reloc_NZ[i : i + 1000] for i in range(0, len(liste_pour_reloc_NZ), 1000)]
    for my_bibs in liste_de_lites :
        set.add_members(my_bibs,'json')
        if set.error_status :
            rediger_envoyer_message(True, "Impossible d'ajouter  less MMSID au jeu pour l'export des notices", set.error_message,job_info=job_infos)
            log_module.error(set.error_message)
            log_module.info("FIN DU TRAITEMENT : impossible de créer le jeu pour l'export des notices")
            exit()

# Lancement du job d'export des notices
parametres_du_job = obtenir_job_export_parameters(set.set_id)
export = AlmaJob.AlmaJob(job_id='M44',operation='run',job_parameters=parametres_du_job,accept="xml",apikey=API_KEY, service=SERVICE)
if export.error_status :
    rediger_envoyer_message(True, "Impossible de lancer l'export des notices", export.error_message,job_info=job_infos)
    log_module.error(export.error_message)
    log_module.info("FIN DU TRAITEMENT : impossible lancer l'export des notices")
    exit()
job_status, status_descr = export.job_is_comleted()
if job_status != 'COMPLETED_SUCCESS' :
    rediger_envoyer_message(True, "Le traitement d'export a terminé sur une anomalie", status_descr, job_info=job_infos)
    log_module.error(status_descr)
    log_module.info("FIN DU TRAITEMENT : Echec du job d'export")
    exit()
log_module.info("Lancement du job d'import pour copie des locale des notices de la CZ")
import_job = AlmaJob.AlmaJob(job_id=JOB_IMPORT_ID,operation='run',job_parameters="{}",accept="json",apikey=API_KEY,service=SERVICE)
job_status, status_descr = import_job.job_is_comleted()
if job_status != 'COMPLETED_SUCCESS' :
    rediger_envoyer_message(True, "Le traitement d'export a terminé sur une anomalie", status_descr, job_info=job_infos)
    log_module.error(status_descr)
    log_module.info("FIN DU TRAITEMENT : Echec du job d'export")
    exit()
rediger_envoyer_message(False, "Tout s'est bien passé", "Pensez à lancer le job des fusions des notices", job_info=job_infos)
 



        
log_module.info("FIN DU TRAITEMENT")
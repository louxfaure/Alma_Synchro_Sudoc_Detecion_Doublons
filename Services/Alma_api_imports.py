# -*- coding: utf-8 -*-
import os

import logging
import xml.etree.ElementTree as ET
from math import *
from Services import Alma_api_fonctions


class AlmaJob_Instance_Id(object):
    """Return l'id de l'instance pour un jour donnée d'un job identifié via son job_id "
    """

    def __init__(self,job_id, date_start,date_end,accept='json', apikey=__apikey__, service='AlmaPy') :
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        self.job_id=job_id
        self.logger = logging.getLogger(service)
        self.logger.debug("log depuis almajob")
        self.appel_api = Alma_api_fonctions.Alma_API(apikey=self.apikey,service=self.service)
        status,response = self.appel_api.request('GET', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/jobs/{}/instances?limit=10&offset=0&submit_date_from={}&submit_date_to={}&status=COMPLETED_SUCCESS'.format(job_id,date_start,date_end),
                                        accept=accept)
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.result = self.appel_api.extract_content(response)
            self.logger.debug(self.result)
            self.job_instance_id = self.get_job_instance_id()
        


    def get_nb_de_jobs(self):
        return self.result['total_record_count']

    def get_job_instance_id(self):
        if 'job_instance' in self.result.keys():
            return self.result['job_instance'][0]['id']
        else :
            return 0
        
    def get_job_infos(self):
        job_infos = {
            "est_erreur" : False,
            "msg_erreur" : "",
            "job_id" : self.job_instance_id,
            "nb_notices_fournies" : 0,
            "nb_notices_chargees" : 0,
            "nb_multi-match" : 0,
            "nb_notices_non_chargees":0
        }
        status,response = self.appel_api.request('GET', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/jobs/{}/instances/{}'.format(self.job_id,self.job_instance_id),
                                        accept='json')
        if status == 'Error':
            job_infos['est_erreur'] = True
            job_infos['msg_erreur'] = response
            return job_infos
        result = self.appel_api.extract_content(response)
        job_infos['nb_notices_fournies'] = result['counter'][0]['value']
        job_infos['nb_notices_chargees'] = result['counter'][1]['value']
        job_infos['nb_notices_non_chargees'] = result['counter'][3]['value']
        job_infos['nb_multi-match'] = result['action'][3]['members']
        return job_infos
    
    def get_multimatch(self,offset,limit=100):
        liste_des_mutimatchs=[]
        status,response = self.appel_api.request('GET', 
                                'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/jobs/{}/instances/{}/matches?population=MULTI_MATCHES&limit={}&offset={}'.format(self.job_id,self.job_instance_id,limit,offset),
                                accept='json')
        if status == 'Error':
            return True, response
        
        result = self.appel_api.extract_content(response)
        for group in result['match']:
           mon_dict = { 
               'ppn' : group['incoming_record_id'],
               'mms_ids' : group['mms_ids'].split(', ')
               }
           liste_des_mutimatchs.append(mon_dict)
        self.logger.debug(liste_des_mutimatchs)
        return False, liste_des_mutimatchs
    

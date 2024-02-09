# -*- coding: utf-8 -*-
import os
# external imports
import requests
import logging
import xml.etree.ElementTree as ET
from math import *
import time
from Services import Alma_api_fonctions


class AlmaJob(object):
    """Lance un job"
    """

    def __init__(self,job_id,operation="run",job_parameters="",accept='xml', apikey=__apikey__, service='AlmaPy') :
        """AlmaJob : init lance un traitement dans Alma. La methode get_job_status() permet de savoir si le job est terminé et si celui-ci est bien terminé.

        Args:
            job_id (_type_): identifiant du traitement
            operation (str, optional): type d'opération. Defaults to "run".
            job_parameters (str, optional): Paramètres à passer au job. Defaults to "".
            accept (str, optional): xml ou json. Defaults to 'xml'.
            apikey (_type_, optional): clef de l'API. Defaults to __apikey__.
            service (str, optional): non du script qui appelle AlmaJob. Permet de récupérer la conf du logger. Defaults to 'AlmaPy'.

        Raises:
            Exception: _description_
        """
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        self.logger = logging.getLogger(service)
        self.appel_api = Alma_api_fonctions.Alma_API(apikey=self.apikey,service=self.service)
        status,response = self.appel_api.request('POST', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/jobs/{}?op={}'.format(job_id,operation),
                                        accept=accept, content_type=accept, data=job_parameters)
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.result = self.appel_api.extract_content(response)
            root = ET.fromstring(self.result)
            self.link_to_job = root.find("./additional_info").attrib['link'] 
            # self.logger.debug(link_to_job)


    def get_job_status(self) :
        self.logger.debug('TRUC')
        job_is_valid_status = {
			'COMPLETED_FAILED':True,
			'COMPLETED_NO_BULKS':True,
			'COMPLETED_SUCCESS':True,
			'COMPLETED_WARNING':True,
			'FAILED':True,
			'FINALIZING':False,
			'INITIALIZING':False,
			'MANUAL_HANDLING_REQUIRED':True,
			'PENDING':False,
			'QUEUED':False,
			'RUNNING':False,
			'SKIPPED':True,
			'SYSTEM_ABORTED':True,
			'USER_ABORTED':True,
        }
        status,response = self.appel_api.request('GET', 
                                        self.link_to_job,
                                        accept='json')
        if status == 'Error':
            return False, 'FAILED', response
        else:
            result = self.appel_api.extract_content(response)
            return job_is_valid_status[result['status']['value']], result['status']['value'], result['status']['desc']
        
    def job_is_comleted(self) :
        while True :
            is_completed, code , response = self.get_job_status()
            if is_completed :
                self.logger.info("Le traitement {} est terminé".format(self.link_to_job))
                return code, response
            self.logger.info("{} : on rappelle le taitement".format(response))
            time.sleep(30)



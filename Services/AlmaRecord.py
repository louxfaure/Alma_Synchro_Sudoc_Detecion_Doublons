# -*- coding: utf-8 -*-
import os
# external imports
import json
import logging
import xml.etree.ElementTree as ET
from math import *
import re
from Services import Alma_api_fonctions



class AlmaRecord(object):
    """A set of function for interact with Alma Apis in area "Records & Inventory"
    """

    def __init__(self, mms_id,id_type='mms_id',view='full',expand='None',accept='json', apikey="", service='AlmaPy') :
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        self.logger = logging.getLogger(service)
        self.appel_api = Alma_api_fonctions.Alma_API(apikey=self.apikey,service=self.service)
        status,response = self.appel_api.request('GET', 
                                  
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/bibs?{}={}&view={}&expand={}'.format(id_type,mms_id,view,expand),
                                        accept=accept)
        self.response = self.appel_api.extract_content(response)
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        elif self.nb_of_records() != 1:
            self.error_status = True
            self.error_message = "l'API retourne 0 ou plusieurs résultats"
        else:
            self.doc = self.response['bib'][0]
            # self.logger.debug(self.doc['bib'][0])       

    def is_cz_record(self):
        if not self.doc['linked_record_id'] :
            return False
        else :
            return True

    def is_unimarc_record(self):
        # self.logger.debug(self.doc)
        if self.doc['record_format'] == 'unimarc' :
            return True
        else :
            return False

    def is_abes_record(self):
        if self.doc['originating_system'] == 'ABES' or re.search(r"SUDOC",self.doc['originating_system']) :
            return True
        else :
            return False
    
    def nb_of_records(self):
        return self.response['total_record_count']
    
    def brief_level(self):
        return self.doc['brief_level']['value'] if 'brief_level' in self.doc else 0

    def titre(self):
        return self.doc['title']
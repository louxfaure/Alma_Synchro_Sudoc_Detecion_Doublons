# -*- coding: utf-8 -*-
import os

import json
import logging
import xml.etree.ElementTree as ET
from math import *
from Services import Alma_api_fonctions


__version__ = '0.1.0'
__apikey__ = os.getenv('ALMA_API_KEY')
__region__ = os.getenv('ALMA_API_REGION')

class AlmaSet(object):
    """Créé un set de notice bib et et l'alimente"
    """

    def __init__(self,create=True,set_id="",nom="",accept='json', apikey=__apikey__, service='AlmaPy') :
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        self.logger = logging.getLogger(service)
        self.set_id = set_id
        if create :
            self.create_set(nom,accept)
        else :  
            self.get_set(set_id,accept)
        
    def create_set(self,name, accept='json') :
        data =  {
            "link":"",
            "name": name,
            "description":"Créé par API par le programme {}".format(self.service),
            "type":{"value":"ITEMIZED"},
            "content":{"value":"BIB_MMS"},
            "private":{"value":"true"},
            "status":{"value":"ACTIVE"},
            "note":"",
            "query":{"value":""},
            "origin":{"value":"UI"}
            }
        self.appel_api = Alma_api_fonctions.Alma_API(apikey=self.apikey,service=self.service)
        status,response = self.appel_api.request('POST', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/sets?combine=None&set1=None&set2=None',
                                        accept=accept, content_type=accept, data=json.dumps(data))
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.set_data = self.appel_api.extract_content(response)
            self.set_id = self.set_data["id"]
            # self.logger.debug(self.set_data)


    def get_set(self,set_id, accept) :
        status,response = self.appel_api.request('GET', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/sets/{}'.format(set_id),
                                        accept=accept)
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.set_data = self.appel_api.extract_content(response)
            # self.logger.debug(self.set_data)

    def add_members(self,mms_ids_list,accept):
        members = [{'id': element} for element in mms_ids_list]
        
        self.set_data["members"] = {
            "member" : members
        }
        self.logger.debug(self.set_data)
        status,response = self.appel_api.request('POST', 
                                'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/conf/sets/{}?op=add_members&fail_on_invalid_id=false'.format(self.set_id),
                                accept=accept, content_type=accept, data=json.dumps(self.set_data))
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.logger.debug(self.appel_api.extract_content(response))


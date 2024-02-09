# -*- coding: utf-8 -*-
import os
import zipfile
import io
# external imports
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import logging
import xml.etree.ElementTree as ET
from math import *
import re


FORMATS = {
    'json': 'application/json',
    'xml': 'application/xml'
}
NS = {'sru': 'http://www.loc.gov/zing/srw/',
        'marc': 'http://www.loc.gov/MARC21/slim',
        'xmlb' : 'http://com/exlibris/urm/general/xmlbeans'
         }

class Alma_API(object):
    """Return l'id de l'instance pour un jour donnée d'un job identifié via son job_id "
    """

    def __init__(self, apikey="", service='AlmaPy') :
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        
        self.logger = logging.getLogger(service)
        self.logger.debug("log depuis almajob")
        

    @property
    #Construit la requête et met en forme les réponses

    def headers(self, accept='json', content_type=None):
        headers = {
            "User-Agent": "pyalma/0.1.0",
            "Authorization": "apikey {}".format(self.apikey),
            "Accept": FORMATS[accept]
        }
        if content_type is not None:
            headers['Content-Type'] = FORMATS[content_type]
        return headers
    def get_error_message(self, response, accept):
        """Extract error code & error message of an API response
        
        Arguments:
            response {object} -- API REsponse
        
        Returns:
            int -- error code
            str -- error message
        """
        error_code, error_message = '',''
        if accept == 'xml':
            root = ET.fromstring(response.text)
            error_message = root.find(".//xmlb:errorMessage",NS).text if root.find(".//xmlb:errorMessage",NS).text else response.text 
            error_code = root.find(".//xmlb:errorCode",NS).text if root.find(".//xmlb:errorCode",NS).text else '???'
        else :
            try :
             content = response.json()
            except : 
                # Parfois l'Api répond avec du xml même si l'en tête demande du Json cas des erreurs de clefs d'API 
                root = ET.fromstring(response.text)
                error_message = root.find(".//xmlb:errorMessage",NS).text if root.find(".//xmlb:errorMessage",NS).text else response.text 
                error_code = root.find(".//xmlb:errorCode",NS).text if root.find(".//xmlb:errorCode",NS).text else '???'
                return error_code, error_message
            if 'web_service_result' in content.keys():
                error_message = content['web_service_result']['errorList']['error']['errorMessage']
                error_code = content['web_service_result']['errorList']['error']['errorCode']
            else :
                error_message = content['errorList']['error'][0]['errorMessage']
                error_code = content['errorList']['error'][0]['errorCode']
        return error_code, error_message
    
    def request(self, httpmethod, url, params={}, data=None,
                accept='json', content_type=None, nb_tries=0, in_url=None):
        #20190905 retry request 3 time s in case of requests.exceptions.ConnectionError
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.request(
            method=httpmethod,
            headers={
            "User-Agent": "pyalma/{}".format(__version__),
            "Authorization": "apikey {}".format(self.apikey),
            "Accept": FORMATS[accept],
            "Content-Type": FORMATS[accept]
        },
            url= url,
            params=params,
            data=data)
        try:
            response.raise_for_status()  
        except requests.exceptions.HTTPError:
            if response.status_code == 400 :
                error_code, error_message= self.get_error_message(response,accept)
                self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
                return 'Error', "{} -- {}".format(error_code, error_message)
            else :
                error_code, error_message= self.get_error_message(response,accept)
            if error_code == "401873" :
                return 'Error', "{} -- {}".format(error_code, "Notice innconnue")
            self.logger.error("Alma_Apis :: HTTP Status: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        except requests.exceptions.ConnectionError:
            error_code, error_message= self.get_error_message(response,accept)
            self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        except requests.exceptions.RequestException:
            error_code, error_message= self.get_error_message(response,accept)
            self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        return "Success", response
    
    def extract_content(self, response):
        ctype = response.headers['Content-Type']
        if 'json' in ctype:
            return response.json()
        else:
            return response.content.decode('utf-8')
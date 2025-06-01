################################################################################
# Module:      api.py
# Description: Module to map the connection to the modules that need an
#              external connection through API (ex. MADE, GDD).
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import requests
import json
from copy import deepcopy

from mosaic_framework.vault.vault import MosaicVault
from mosaic_framework.vault.secret import APIKeySecret
from mosaic_framework.data_layer.data_retriever_protocol import ProtocolDataRetriever
from mosaic_framework.data_layer.exceptions import ParameterNotAllowedException, APIPermissionException, APIServiceInternalException

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory

    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

class GetApi(ProtocolDataRetriever):
    def __init__(self, **kwargs) -> None:
        self.stage = kwargs.get("stage", "develop")
        self.vault = MosaicVault()
    
    @staticmethod
    def get_domain(api_url:str)->str:
        """
        Get the domain from the API URL.
        """
        return api_url.replace('https://', '').split('/')[0].split(".")[0]
    
    @staticmethod
    def get_mandatory_keys(path_params_part:str)->List[str]:
        """
        Get from path parameters the keys that are mandatory.
        """
        updt_path_params_part = deepcopy(path_params_part)
        updt_path_params_part = updt_path_params_part.replace("https://", "")
        updt_path_params_part = updt_path_params_part.split("/")
        return [s.replace('{', '').replace('}', '') for s in updt_path_params_part if '{' in s and '}' in s]
        
    @staticmethod
    def get_optional_keys(query_params_part:str)->List[str]:
        """
        Get from query parameters the keys that are optional.
        """
        updt_query_params_part = deepcopy(query_params_part)
        updt_query_params_part = updt_query_params_part.split("&")
        
        return [s.split('=')[1].replace('{', '').replace('}', '') for s in updt_query_params_part] 

    def get_allowed_keys(self, api_url:str)->List[str]:
        """
        Process the API URL and return a list of allowed keys, based on 
        the path parameters and query parameters that are found.
        """
        return self.get_mandatory_keys(path_params_part=api_url.split("?")[0]) + \
               self.get_optional_keys(query_params_part=api_url.split("?")[1]) \
                if api_url.find("?") != -1 \
                else self.get_mandatory_keys(path_params_part=api_url.split("?")[0])
    
    @staticmethod
    def remove_empty_query_param(api_url:str, query_param_name:str):
        return api_url.replace(f"&{query_param_name}", "") if f"&{query_param_name}" in api_url \
            else api_url.replace(f"{query_param_name}", "")
    
    def build(self, api_url:str, params:dict)->None:
        #for each item in params update the url
        updt_url = deepcopy(api_url)
        
        path_params  = self.get_mandatory_keys(path_params_part=updt_url.split("?")[0]) \
            if updt_url.find("?")!=-1 \
            else self.get_mandatory_keys(path_params_part=updt_url)
        query_params = self.get_optional_keys(query_params_part=updt_url.split("?")[1]) \
            if updt_url.find("?")!=-1 \
            else []

        print(f"[GetApi] path_params={path_params}")
        print(f"[GetApi] query_params={query_params}")

        #Path params are mandatory, so if we cannot find them raise error, 
        #otherwise replace them in the url
        for path_param in path_params:
            if path_param in params.keys():
                updt_url = updt_url.replace("{"+path_param+"}", str(params[path_param]))
            else:
                raise ParameterNotAllowedException(f"Missing parameter {path_param} in params")

        #Query params are optional, so if we find them, replace them in the url
        #otherwise remove the query param
        for query_param in query_params:
            if query_param in params.keys():
                updt_url = updt_url.replace("{"+query_param+"}", str(params[query_param]))
                #also we need to check if params[path_param] is None or '', because if
                #it is then we need to remove the queryParam
                if params[query_param] == None or params[query_param] == '':
                    updt_url = self.remove_empty_query_param(api_url=updt_url, query_param_name=query_param)
            else:
                #Removing the query param based on the presence of '&', cause
                #we do not know if it is a first element.
                if "&"+query_param in updt_url:
                    updt_url = updt_url.replace("&"+query_param+"={"+str(query_param)+"}", "")
                else:
                    updt_url = updt_url.replace(query_param+"={"+str(query_param)+"}", "")
        
        return updt_url

    def retrieve(self, api_url:str, api_parameters:dict)->None:
        #Get all the keys that can be set in the API URL
        allowed_keys   = self.get_allowed_keys(api_url=api_url)
        service_domain = self.get_domain(api_url=api_url)
        print(f"[GetApi] api_url={api_url}")
        print(f"[GetApi] allowed_keys={allowed_keys}")
        
        #Checking if parameters in api_parameters are allowed
        for k in api_parameters.keys():
            if k not in allowed_keys:
                raise ParameterNotAllowedException(f"Invalid key {k} in api_parameters")
        
        updt_url = self.build(api_url=api_url, params=api_parameters)
        print(f"[GetApi] Builted api_url={updt_url}")

        #Get the response and deal with it.
        #x-api-key needed is got from the Vault
        #secret_name: str that is used to map a secret, 
        #             got from MosaicVault
        result      : requests.Response = \
            requests.get(
                url=updt_url, 
                headers={'x-api-key': self.vault.retrieve_secret(secret_class=APIKeySecret, secret_name=service_domain, stage=self.stage)})
        status_code : int = result.status_code
        body        : str = result.text

        if status_code == 403:
            raise APIPermissionException(f"API call lack of permission: Message got: {json.dumps(body, default=str)}")
        
        if status_code == 503:
            raise APIServiceInternalException(f"API call is not available or got an error: Message got: {json.dumps(body, default=str)}")

        return {'status_code': status_code, 'body': body}

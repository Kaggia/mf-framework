################################################################################
# Module:      susceptibility.py
# Description: SubComponent to work with Disease susceptibilities, it interacts,
#              directly with DiseaseV2.
# Author:      Stefano Zimmitti
# Date:        21/11/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, Any, TYPE_CHECKING
from copy import deepcopy
import datetime
import pandas as pd
import pkgutil
import importlib
import json
import os

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

import mosaic_framework
import mosaic_framework.data_layer
from mosaic_framework.config.configuration import SUSCEPTIBILITY
from mosaic_framework.config.data_layer_configuration import SUSCEPTIBILITY_API_URL
from mosaic_framework.engine.exceptions import ClassNotFoundException
from mosaic_framework.agronomics.exceptions import DataFormatException
from mosaic_framework.retrieving.exceptions import DataBridgeConnectionException
from mosaic_framework.components.components import InternalComponent
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.data_storage.converters import Converter

class Susceptibility(InternalComponent):
    """
    Handles disease susceptibility calculations for crops by interfacing with the DiseaseV2 service.

    This class manages the susceptibility of a crop to diseases by retrieving data from the 
    DiseaseV2 service's susceptibility API. It processes agronomic data and refines input data
    to determine susceptibility levels.

    The stage parameter refers to the API environment (e.g. test, prod), not the database schema.
    Database schema selection is handled by the called API service.

    Attributes:
        tag (str): Component tag identifier, set to 'data'
        exec_priority (int): Execution priority level, set to 0 
        data_storage (MosaicDataStorageType): Data storage interface
        shared_memory (MosaicSharedMemoryType): Shared memory interface
        stage (str): API environment stage (defaults to env var or 'test')
        commodity_id (int): ID of the crop/commodity
        variety_id (int): ID of the crop variety
        disease_id (int): ID of the disease
        mapping (dict): Optional mapping configuration
        data_source (str): Data source type, set to 'api'
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=SUSCEPTIBILITY, **kwargs)
        self.tag          = 'data'
        self.exec_priority    = 0
        self.data_storage     = None
        self.shared_memory    = None    
        self.stage            = kwargs.get('stage', os.getenv('dev_environment', 'test'))
        self.commodity_id     = kwargs.get('commodity_id', None)
        self.variety_id       = kwargs.get('variety_id', None)
        self.disease_id       = kwargs.get('disease_id', None)
        self.mapping          = kwargs.get('mapping', None)
        self.data_source      = 'api'

    @staticmethod
    def get_modules()->list:
        """
        Discovers and loads all modules in the mosaic_framework.data_layer package.

        Walks through the package to find any data layer modules and adds them to a list.
        Modules are discovered dynamically at runtime.

        Returns:
            List[Any]: List of discovered modules in the data layer path
        """
        # Scorri tutti i moduli nel package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.data_layer.__path__, mosaic_framework.data_layer.__name__ + '.'):
            # Importa il modulo dinamicamente
            module = importlib.import_module(module_name)
            # Aggiungi il modulo alla lista
            modules.append(module)

        return modules
    
    def get_data_retriever_object(self, http_request_type:str="get")->Any:
        """
        Retrieves the appropriate data retriever object based on HTTP request type.

        Args:
            http_request_type (str): HTTP method type (e.g. "get", "post"). Defaults to "get".

        Returns:
            Any: Instantiated data retriever object

        Raises:
            ClassNotFoundException: If no matching retriever class is found
        """
        modules                  = self.get_modules()
        data_retriever_classname = f"{str(http_request_type).capitalize()}{str(self.data_source).capitalize()}"
        for m in modules:
            if hasattr(m, data_retriever_classname):
                cls = getattr(m, data_retriever_classname)
                obj = cls(stage=self.stage) 
                print(f'[Susceptibility]: Oggetto creato della classe: {data_retriever_classname}.')
                return obj
        raise ClassNotFoundException(f"Class: {data_retriever_classname} cannot be found in modules available.")
    
    def get_sources_to_update(self)->List[dict]:
        """
        Finds connectors linking Source components (weather data) to Model components.

        Returns:
            List[dict]: List of connector configurations linking Sources to Models
        """
        components_image    = self.shared_memory.get_variable('components_image').content
        connectors          = self.shared_memory.get_variable('connectors').content
        
        #Getting all the images that are Model
        model_images        = [ci_label for ci_label, ci_type in components_image.items() if ci_type == 'Model']
        source_images       = [ci_label for ci_label, ci_type in components_image.items() if ci_type == 'Source']
        #Getting all connectors that are connected to Model(s) and are Source
        filtered_connectors = [c for c in connectors if c['connect_out'] in model_images and c['connect_in'] in source_images]
        
        return filtered_connectors
 
    def get_date_column(self):
        """
        Gets the default date column name from shared memory.

        Retrieves the date column mapping from the global columns mapping configuration
        in shared memory.

        Returns:
            str: The mapped date column name

        Raises:
            DataBridgeConnectionException: If no connector is found for the parent component
            DataFormatException: If no source connectors are found for mapping
        """
        #example of connectors:         [{'connect_in': 'test_source', 'connect_out': 'agro_model', 'resource': <obj>}]
        #example of components_image:   {'test_source': 'Source', 'validation_source': 'Source', 'hazelnut': 'Colture', 'source_to_model_databridge': 'DataBridge', 'source_to_validator_databridge': 'DataBridge', 'model_to_validator_databridge': 'DataBridge', 'colture_to_model_databridge': 'DataBridge', 'agro_model': 'Model', 'model_validator': 'Validator', 'JKvUQ': 'ModelValidation'}
        connectors               = self.shared_memory.get_variable(key='connectors', error_policy='raise').content
        components_image         = self.shared_memory.get_variable(key='components_image', error_policy='raise').content

        #get the connectors that have as connect_out the parent component (self)
        parent_connector      = [c for c in connectors if c['connect_in'] == self.parent]
        if len(parent_connector) != 1:
            raise DataBridgeConnectionException(f"There are no connectors that have '{self.parent}' as input connection.")
        #filtering connectors that have connect_out equal to the parent.
        c_out_connectors         = [c for c in connectors if c['connect_out'] == parent_connector[0]['connect_out']]
        #filtering all the connectors found, that have also Source type as connect_in.
        filtered_connectors      = [c for c in c_out_connectors if components_image[c['connect_in']] == 'Source']
        if len(filtered_connectors) == 0:
            raise DataFormatException("There are no connectors, in order to get the mapping from 'global_columns_sources_mapping'.")
        global_columns_mapping = self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='raise').content
        mapping                = global_columns_mapping[filtered_connectors[0]['connect_in']]
        #revert the mapping, in order to get the column name from the data
        mapping                = {v:k for k,v in mapping.items()}
        print(f"[Susceptibility] Column date has been retrieved from mapping: {mapping['SampleDate']}")
        return mapping['SampleDate']
    
    def get_updated_data(self, data:pd.DataFrame, mapping:dict, susceptibility_value:str):
        """
        Appends susceptibility data to the input DataFrame.

        Args:
            data (pd.DataFrame): Input DataFrame to update
            mapping (dict): Mapping of susceptibility values
            susceptibility_value (str): Key for susceptibility value in mapping

        Returns:
            pd.DataFrame: Updated DataFrame with susceptibility column added
        """
        updt_data = deepcopy(data)
        updt_data['susceptibility'] = mapping[susceptibility_value]

        return updt_data

    def retrieve(self):
        """
        Retrieves susceptibility data from the configured data source.

        Makes an API call to the susceptibility service with the configured parameters.

        Returns:
            dict: JSON response containing susceptibility data
        """
        api_retriever = self.get_data_retriever_object(http_request_type="get")
        data = api_retriever.retrieve(
            api_url=SUSCEPTIBILITY_API_URL,
            api_parameters={
                'commodity_id': self.commodity_id, 
                'variety_id':self.variety_id, 
                'disease_id':self.disease_id,
                'stage': self.stage})['body']

        return json.loads(data)
    
    def run(self) -> None:
        #Getting the commodity from the parent.
        susceptibility_data  = self.retrieve()
        print(f"[Susceptibility]: Data got:\n{json.dumps(susceptibility_data, indent=4)}")
        #here you can dealt with eventual changes or update to the
        #growth model data got from Data Layer

        #Also we update the Source resource that will go directly to the Model
        #input, we intend to add directly columns to the input dataframe.
        connectors = self.get_sources_to_update()
        for i, c in enumerate(connectors):
            raw_data     = self.data_storage.get_resource(label=c['connect_in']).get_data()
            converter    = Converter()
            source_data  = converter.to_data_format(data=raw_data, data_format='dataframe')
            updt_data    = self.get_updated_data(data=source_data, mapping=susceptibility_data['data']['mapping'], susceptibility_value=susceptibility_data['data']['result'])
            #Update the Resource with the new data
            new_resource = Resource(label=c['connect_in'], data=updt_data, file_type='csv')
            self.data_storage.replace_resource(new_resource=new_resource)
            #Update the Connector with the new data
            connectors_to_update = self.shared_memory.get_variable(key='connectors').content
            for i, ctu in enumerate(connectors_to_update):
                if connectors_to_update[i]['connect_in'] == c['connect_in']:
                    connectors_to_update[i]['resource'] = new_resource
                    break
            self.shared_memory.update_variable(key='connectors', new_content=connectors_to_update)
        print("[Susceptibility]: Closed.")
        return updt_data

    def __str__(self):
        return f'{str(type(self))[str(type(self)).rfind(".")+1:]}:{self.__dict__}'.replace("'>", "")

################################################################################
# Module:      growth_models.py
# Description: Collection of Components that allow to handle the Growth Model 
#              agronomic part. Contains base GrowthModel class and specialized
#              implementations for different growth model types.
# Author:      Stefano Zimmitti
# Date:        ??/07/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, Any, Dict, TYPE_CHECKING
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
from mosaic_framework.config.data_layer_configuration import PHENOSTAGE_API_URL, INSURTECH_API_URL
from mosaic_framework.engine.exceptions import ClassNotFoundException
from mosaic_framework.agronomics.exceptions import DataFormatException
from mosaic_framework.retrieving.exceptions import DataBridgeConnectionException
from mosaic_framework.components.sub_component import SubComponent
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.data_storage.converters import Converter

class GrowthModel(SubComponent):
    """
    GrowthModel class is used to handle the Growth Model agronomic part.
    It is directly connected to GDD service, in particular with phenostage API.
    
    This is the base class for different growth model implementations. It provides
    common functionality for retrieving and processing growth model data.

    Args:
        data_storage (MosaicDataStorageType): Storage object for model data
        shared_memory (MosaicSharedMemoryType): Shared memory for component communication
        data_source (str): Source of growth model data (e.g. 'api')
        colture_data (dict): Dictionary containing crop/culture parameters
        **kwargs: Additional keyword arguments
            - parent: Parent component reference
            - stage: Development stage, defaults to environment variable 'dev_environment'
    """

    def __init__(self, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType, data_source:str, colture_data:dict, **kwargs) -> None:
        super().__init__(parent=kwargs.get('parent', None))
        self.data_storage     = data_storage
        self.shared_memory    = shared_memory
        self.data_source      = data_source
        self.colture_data     = colture_data
        self.stage            = kwargs.get('stage', os.getenv('dev_environment', 'develop'))
        
    
    @staticmethod
    def get_modules() -> List[Any]:
        """
        Run over mosaic_framework.retrieving.connectors package, find any Connector that is part of the package
        then add them to a list. Modules are found dynamically at runtime.

        Returns:
            List[Any]: List of dynamically loaded connector modules from mosaic_framework.data_layer
        """
        # Iterate through all modules in package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.data_layer.__path__, mosaic_framework.data_layer.__name__ + '.'):
            # Import module dynamically
            module = importlib.import_module(module_name)
            # Add module to list
            modules.append(module)

        return modules
    
    def get_data_retriever_object(self, http_request_type:str="get") -> Any:
        """
        Retrieves the correct data retriever object based on request type and data source.

        Args:
            http_request_type (str, optional): HTTP request type ('get', 'post' etc). Defaults to "get".

        Returns:
            Any: Instantiated data retriever object

        Raises:
            ClassNotFoundException: If matching retriever class cannot be found
        """
        modules                  = self.get_modules()
        data_retriever_classname = f"{str(http_request_type).capitalize()}{str(self.data_source).capitalize()}"
        for m in modules:
            if hasattr(m, data_retriever_classname):
                cls = getattr(m, data_retriever_classname)
                obj = cls(stage=self.stage) 
                print(f'[GrowthModel]: Oggetto creato della classe: {data_retriever_classname}.')
                return obj
        raise ClassNotFoundException(f"Class: {data_retriever_classname} cannot be found in modules available.")
    
    def get_sources_to_update(self) -> List[Dict]:
        """
        Retrieves connectors that link Source components to Model components.

        Returns:
            List[dict]: List of connector dictionaries containing connection details
        """
        components_image    = self.shared_memory.get_variable('components_image').content
        connectors          = self.shared_memory.get_variable('connectors').content
        
        #Getting all the images that are Model
        model_images        = [ci_label for ci_label, ci_type in components_image.items() if ci_type == 'Model']
        source_images       = [ci_label for ci_label, ci_type in components_image.items() if ci_type == 'Source']
        #Getting all connectors that are connected to Model(s) and are Source
        filtered_connectors = [c for c in connectors if c['connect_out'] in model_images and c['connect_in'] in source_images]
        
        return filtered_connectors
 
    def get_date_column(self) -> str:
        """
        Gets the default date column name from available options in data.
        Looks for standard date column names like "sampledate", "date", etc.

        Returns:
            str: Name of identified date column

        Raises:
            DataBridgeConnectionException: If no valid connector found
            DataFormatException: If no valid mapping found
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
        print(f"[GrowthModel] Column date has been retrieved from mapping: {mapping['SampleDate']}")
        return mapping['SampleDate']
    
    def retrieve(self) -> Dict:
        """
        Retrieves growth model data from configured data source.

        Returns:
            Dict: Growth model data in JSON format
        """
        api_retriever = self.get_data_retriever_object(http_request_type="get")
        data = api_retriever.retrieve(
            api_url=PHENOSTAGE_API_URL,
            api_parameters=self.colture_data)['body']

        return json.loads(data)
    
    def run(self) -> Dict:
        """
        Main execution method that retrieves and processes growth model data.

        Returns:
            Dict: Processed growth model data
        """
        super().run()
        print("[GrowthModel]: Running...")
        
        data = self.retrieve()
        return data
    
    def __str__(self) -> str:
        """Returns string representation of the growth model instance"""
        return f'{str(type(self))[str(type(self)).rfind(".")+1:]}:{self.__dict__}'.replace("'>", "")

class FixedGrowthModel(GrowthModel):
    """
    Implementation for fixed growth models that use time deltas from seeding.
    These models have predefined growth stages based on calendar dates rather than 
    growing degree days.

    Args:
        data_storage (MosaicDataStorageType): Storage object for model data
        shared_memory (MosaicSharedMemoryType): Shared memory for component communication 
        colture_data (dict): Dictionary containing crop/culture parameters
        data_source (str, optional): Source of growth model data. Defaults to "api".
        **kwargs: Additional keyword arguments
            - parent: Parent component reference
    """
    def __init__(self, data_storage: MosaicDataStorageType, shared_memory: MosaicSharedMemoryType, colture_data:dict, data_source:str="api", **kwargs) -> None:
        super().__init__(data_storage=data_storage, shared_memory=shared_memory, data_source=data_source, colture_data=colture_data, parent=kwargs.get('parent', None))
    
    def get_updated_data(self, source_data:pd.DataFrame, growth_model:List[Dict]) -> pd.DataFrame:
        """
        Merges source data with growth model data to create enriched dataset.
        Adds phenological stage information to each data point based on date.

        Args:
            source_data (pd.DataFrame): Original source data
            growth_model (List[dict]): Growth model stage definitions

        Returns:
            pd.DataFrame: Source data enriched with growth model information
        """
        def append_growth_model_data(sample_date, growth_model, key:str):
            for i, pheno in enumerate(growth_model):
                if pheno['start'].year == pheno['end'].year:
                    pheno_start=datetime.datetime(year=sample_date.year, month=pheno['start'].month, day=pheno['start'].day)
                    pheno_end=datetime.datetime(year=sample_date.year, month=pheno['end'].month, day=pheno['end'].day)
                else:
                    pheno_start=datetime.datetime(year=sample_date.year, month=pheno['start'].month, day=pheno['start'].day)
                    pheno_end=datetime.datetime(year=sample_date.year+1, month=pheno['end'].month, day=pheno['end'].day)
                if sample_date>=pheno_start and sample_date<=pheno_end:
                    return growth_model[i][key]
            return -1
        """
        Merge source data with growth model data. Creating two columns: 
        - growth_model_id  : the id of the growth model
        - growth_model_name: the name of the growth model
        """
        #growth_model is expressed as a dictionary where each item is:
        # {
        #     "pheno_id": 2302,
        #     "start"   : "1970-02-15",
        #     "end"     : "1970-04-21",
        #     "pheno_signature_it": "SV",
        #     "pheno_name_it"     : "Sviluppo vegetativo",
        #     "pheno_name_en"     : "Leaf and shoot development"
        # }
        #While source_data is expressed as a pd.Dataframe.
        date_column                = self.get_date_column()
        
        updt_growth_model          = deepcopy(growth_model)
        updt_source_data           = deepcopy(source_data)
        
        for i, pheno in enumerate(updt_growth_model):
            updt_growth_model[i]['start'] = pd.to_datetime(updt_growth_model[i]['start'])
            updt_growth_model[i]['end']   = pd.to_datetime(updt_growth_model[i]['end'])
        
        updt_source_data[date_column+"_dt"] = pd.to_datetime(updt_source_data[date_column])

        updt_source_data['pheno_id']        = updt_source_data[date_column+"_dt"].apply(lambda x:append_growth_model_data(x, updt_growth_model, 'pheno_id'))
        updt_source_data['pheno_name_en']   = updt_source_data[date_column+"_dt"].apply(lambda x:append_growth_model_data(x, updt_growth_model, 'pheno_name_en'))
        
        return updt_source_data
    
    def run(self) -> Dict:
        """
        Main execution method that retrieves growth model data and updates source data.
        
        Returns:
            Dict: Retrieved and processed growth model data
        """
        data = super().run()
        print(f"[FixedGrowthModel]: Data got:\n{json.dumps(data, indent=4)}")
        #here you can dealt with eventual changes or update to the
        #growth model data got from Data Layer

        #Also we update the Source resource that will go directly to the Model
        #input, we intend to add directly columns to the input dataframe.
        connectors = self.get_sources_to_update()
        for i, c in enumerate(connectors):
            raw_data     = self.data_storage.get_resource(label=c['connect_in']).get_data()
            converter    = Converter()
            source_data  = converter.to_data_format(data=raw_data, data_format='dataframe')
            updt_data    = self.get_updated_data(source_data=source_data, growth_model=data)
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
        print("[FixedGrowthModel]: Closed.")
        return data

class GDDGrowthModel(GrowthModel):
    """
    Growth model implementation based on Growing Degree Days (GDD).
    Handles growth stages based on accumulated temperature units.
    
    Args:
        data_storage (MosaicDataStorageType): Storage object for model data
        shared_memory (MosaicSharedMemoryType): Shared memory for component communication
        colture_data (dict): Dictionary containing crop/culture parameters
        data_source (str, optional): Source of growth model data. Defaults to "api".
    """
    def __init__(self, data_storage: MosaicDataStorageType, shared_memory: MosaicSharedMemoryType, colture_data:dict, data_source:str="api") -> None:
        super().__init__(data_storage=data_storage, shared_memory=shared_memory, data_source=data_source, colture_data=colture_data)

class PolicyGrowthModel(GrowthModel):
    """
    Growth model implementation based on policy/rules rather than fixed dates or GDD.
    
    Args:
        data_storage (MosaicDataStorageType): Storage object for model data
        shared_memory (MosaicSharedMemoryType): Shared memory for component communication
        colture_data (dict): Dictionary containing crop/culture parameters
        data_source (str, optional): Source of growth model data. Defaults to "api".
        **kwargs: Additional keyword arguments
            - parent: Parent component reference
    """
    def __init__(self, data_storage: MosaicDataStorageType, shared_memory: MosaicSharedMemoryType, colture_data:dict, data_source:str="api", **kwargs) -> None:
        colture_data = {**colture_data, 'stage': 'dev' if colture_data.get('stage') is None or 'stage' not in colture_data else colture_data['stage']}
        super().__init__(data_storage=data_storage, shared_memory=shared_memory, data_source=data_source, colture_data=colture_data, parent=kwargs.get('parent', None), stage='dev' if colture_data.get('stage') is None or 'stage' not in colture_data else colture_data['stage'])
        
    def retrieve(self) -> List[Dict]:
        """
        Retrieves policy-based growth model data from configured data source.
        
        Returns:
            List[Dict]: List of growth stage definitions from policy service
        """
        api_retriever = self.get_data_retriever_object(http_request_type="get")
        #removing unused colture_data params
        #Cause of the policy growth model, we don't need to pass model_type and start_date
        #While for the other growth models, we need to pass them.
        self.colture_data.pop('model_type') \
            if 'model_type' in self.colture_data \
            else None
        self.colture_data.pop('start_date') \
            if 'start_date' in self.colture_data \
            else None
        
        #Retrieving data from the Data Layer
        data = api_retriever.retrieve(
            api_url=INSURTECH_API_URL,
            api_parameters=self.colture_data).get('body', "{'data':[]}")
        

        return json.loads(data).get('data', [])
    
    def get_standard_data(self, data:List[Dict]) -> List[Dict]:
        """
        Standardizes policy data format to match common growth model structure.
        
        Args:
            data (List[Dict]): Raw policy data

        Returns:
            List[Dict]: Standardized growth stage data
        """
        updt_data = deepcopy(data)
        for i, item in enumerate(updt_data):
            updt_data[i]['pheno_phase_id']    = int(updt_data[i].pop('phase_id'))
            updt_data[i]['pheno_phase_name']  = updt_data[i].pop('name')
            updt_data[i]['pheno_phase_start'] = float(updt_data[i].pop('start'))
            updt_data[i]['pheno_phase_end']   = float(updt_data[i].pop('end'))
            updt_data[i]['pheno_phase_unit']  = updt_data[i].pop('unit')

        return updt_data
    
    def run(self) -> Dict:
        """
        Main execution method that retrieves and standardizes policy-based growth model data.
        Updates shared lookup table with standardized data.
        
        Returns:
            Dict: Raw policy growth model data
        """
        data = super().run()
        print(f"[PolicyGrowthModel]: Data got:\n{json.dumps(data, indent=4)}")

        #Post processing the data got from the Data Layer
        standard_data = self.get_standard_data(data=data)

        print(f"[PolicyGrowthModel]: Standard data got:\n{json.dumps(standard_data, indent=4)}")

        #Get the actual content of the v_look_up_table
        #If the variable is not present, create it
        v_look_up_table = self.shared_memory.get_variable(key='v_look_up_table', error_policy='pass')
        if v_look_up_table is None:
            self.shared_memory.add_variable(key='v_look_up_table', content={}, is_immutable=False)

        v_look_up_table = self.shared_memory.get_variable(key='v_look_up_table', error_policy='pass').content
        v_look_up_table['growth_model'] = standard_data
        self.shared_memory.update_variable(key='v_look_up_table', new_content=v_look_up_table)
        print("[PolicyGrowthModel]: Closed.")
        return data

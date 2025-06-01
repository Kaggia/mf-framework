################################################################################
# Module:      validator.py
# Description: Validation Component entry point, that launches the validation 
#              processing.
# Author:      Stefano Zimmitti
# Date:        28/06/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import json
import pkgutil
import importlib
import pandas as pd

import mosaic_framework
from mosaic_framework.config.configuration import VALIDATOR
from mosaic_framework.components.components import Component
from mosaic_framework.validation.activity import ValidationActivity
from mosaic_framework.model.exceptions import DataFormatException
from mosaic_framework.engine.exceptions import ClassNotFoundException

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.data_storage.resource import Resource
    
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory
    ResourceType           = Resource

class Validator(Component):
    """
    ---\n
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=VALIDATOR, **kwargs)
        self.tag              = 'postprocess'
        self.data_storage     : MosaicDataStorageType   = None
        self.shared_memory    : MosaicSharedMemoryType  = None
        self.activity         : ValidationActivity      = self.get_activity_obj(self.activity)
    
    @staticmethod
    def get_activity_obj(activity:str):
        """
        @staticmethod 
        Used to get the correct activity from a collection of ValidationActivity.
        ----\n
        params:
        activity:str string needed to look for the activity
        """

        def get_activity_modules()->list:
            """
            Run over mosaic_framework package, find any module that is part of the package
            then add them to a list. If self.DEBUG=True then displays the found modules. 
            Modules are found dynamically at runtime.
            ---\n
            params:
            None
            ---
            returns: List of modules find in mosaic_framework path.
            """
            # Scorri tutti i moduli nel package
            modules = list()
            for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.validation.__path__, mosaic_framework.validation.__name__ + '.'):
                # Importa il modulo dinamicamente
                module = importlib.import_module(module_name)
                # Aggiungi il modulo alla lista
                modules.append(module)    
            return modules

        def import_and_create_objects(modules: List[object], activity: str) -> ValidationActivity:
            """
            Allow to match the objects found in module (class_calls) with any module
            find in modules. Then it returns a list of objects that match, otherwise
            a ClassNotFoundException is raised.
            """
            obj = None
            obj_found = False
            for m in modules:
                if hasattr(m, activity['func']):
                    cls = getattr(m, activity['func'])
                    if issubclass(cls, Component):
                        obj = cls(*activity['args'], **activity['kwargs'])
                    obj_found = True
            if not obj_found:
                raise ClassNotFoundException(f"Class: {activity['func']} cannot be found in modules available.")
            return obj

        activity_modules = get_activity_modules()
        activity_obj     = import_and_create_objects(modules=activity_modules, activity=activity)
        return activity_obj

    def get_involved_connectors(self)->List[dict]:
        """
        Get the list of all involved correctors. An involved connector is one that actually involved in 
        the Model processing, so that has connect_in as Model type.
        ---\n
        params:
        None
        ---\n
        """
        connectors       = self.shared_memory.get_variable(key='connectors',       error_policy='raise').content
        components_image = self.shared_memory.get_variable(key='components_image', error_policy='raise').content
        #Now we need to filter the connector that c['connect_out'] == self.label
        #But also we need to check the the stub type, in facts we need that 
        #c['connect_in'] is a Model Component
        involved_connectors = list()
        for c in connectors:
            if c['connect_out'] == self.label \
                and components_image[c['connect_in']]=='Model':
                    involved_connectors.append(c)

        #Returning all the involved connectors, that we gonna need for the validation
        #activities
        return involved_connectors
    
    def get_validation_data(self) -> pd.DataFrame:
        """
        Get the validation data, from the connector that holds the data in SharedMemory.
        ---\n
        params:
        None
        ---\n
        """

        connectors       = self.shared_memory.get_variable(key='connectors',       error_policy='raise').content
        components_image = self.shared_memory.get_variable(key='components_image', error_policy='raise').content
        
        data = None
        for c in connectors:
            if c['connect_out'] == self.label \
                and components_image[c['connect_in']]=='Source':
                    data = c['resource'].get_data()

        return data
    
    def get_model_data(self, involved_connector:dict) -> pd.DataFrame:
        """
        Get the validation data, from the connector that holds the data in SharedMemory. Specifically
        is got from a subset of connectors (involved connectors)
        ---\n
        params:
        None
        ---\n
        """

        connectors       = self.shared_memory.get_variable(key='connectors',       error_policy='raise').content

        for c in connectors:
            if c['connect_out'] == self.label \
                and c['connect_in']==involved_connector['connect_in']:
                    results         = self.data_storage.get_resource(label=c['resource']['results_pointer']).get_data()
                    compact_results = self.data_storage.get_resource(label=c['resource']['compact_results_pointer']).get_data()

        return {'results':results, 'compact_results':compact_results}
    
    def prepare(self)->None:
        """
        ---\n
        params:
        ---\n
        returns: 
        """

        super().prepare()

        return
    
    def run(self)->None:
        """
        Entry point behaviour of the class. Based on the 'params', elaborate all the rules
        specified, and validate results.
        ---\n
        params:
        None
        ---\n
        Returns:
        - None
        """
        super().run()
        self.prepare()

        #Get involved_connectors, each one of them describe a differet
        #validation Activity, each one has a different result.
        involved_connectors = self.get_involved_connectors()

        for involved_connector in involved_connectors:
            new_activity = self.activity
            new_activity.model_data      = self.get_model_data(involved_connector=involved_connector)
            new_activity.validation_data = self.get_validation_data()
            new_activity.report_flag     = self.report
            new_activity.set_memory(shared_memory=self.shared_memory)
            new_activity.set_storage(data_storage=self.data_storage)
            validation_result : pd.DataFrame = new_activity.run()
            validation_result.to_csv(f"results/results_validation_{involved_connector['connect_in']}_{involved_connector['connect_out']}.csv")
        
        print("[Validator] Closed running.")
        return
    
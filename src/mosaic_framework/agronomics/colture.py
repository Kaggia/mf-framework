################################################################################
# Module:      colture.py
# Description: Component to handle the agronomics.
# Author:      Stefano Zimmitti
# Date:        31/07/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING, Any
from copy import deepcopy
import dateutil
import pkgutil
import importlib
import datetime
import os
import dateutil.parser
import pandas as pd
import warnings
import json

import mosaic_framework
import mosaic_framework.agronomics
from mosaic_framework.config.configuration import COLTURE
from mosaic_framework.components.components import Component
from mosaic_framework.agronomics.susceptibility import Susceptibility
from mosaic_framework.engine.exceptions import ClassNotFoundException
from mosaic_framework.agronomics.exceptions import StartDateValueException
if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory


class Colture(Component):
    """
    Component that handles the agronomics part of the process.
    
    Based on the commodity, destination_use_id, precocity_id and other parameters,
    it instantiates the appropriate SubComponent(s) to retrieve necessary data via
    the data layer. The primary SubComponent is GrowthModel which handles growth
    modeling, but additional SubComponents can be added.

    Attributes:
        tag (str): Component tag identifier
        exec_priority (int): Execution priority level
        is_unique (bool): Whether component should be unique
        data_storage (MosaicDataStorageType): Data storage interface
        shared_memory (MosaicSharedMemoryType): Shared memory interface
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize the Colture component.

        Args:
            **kwargs: Arbitrary keyword arguments passed to parent Component
        """
        super().__init__(configuration=COLTURE, **kwargs)
        self.tag = 'data'
        self.exec_priority = 1
        self.is_unique = True
        self.data_storage: MosaicDataStorageType = None
        self.shared_memory: MosaicSharedMemoryType = None

    @staticmethod
    def get_modules() -> List[Any]:
        """
        Dynamically discover and load all modules in the mosaic_framework.retrieving.connectors package.

        Walks through the package to find any Connectors and adds them to a list.
        Modules are discovered at runtime.

        Returns:
            List[Any]: List of discovered modules in the connectors path
        """
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.agronomics.__path__, mosaic_framework.agronomics.__name__ + '.'):
            module = importlib.import_module(module_name)
            modules.append(module)
        return modules
    
    def get_extra_params(self) -> List[str]:
        """
        Retrieve extra parameters not present in the configuration.

        Returns:
            List[str]: List of parameter names that are not in config or reserved
        """
        config = self.config
        config_params = list(config['params'].keys())
        reserved_params = ['tag', 'data_storage', 'shared_memory', 'params']
        extra_params = [k for k in self.__dict__.keys() if k not in reserved_params and k not in config_params]
        return extra_params
    
    def get_start_date(self) -> str:
        """
        Retrieve and validate the start date.

        Returns:
            str: Formatted start date string or empty string if no date

        Raises:
            StartDateValueException: If start date format is invalid
        """
        def is_valid_datetime(date_str: str) -> bool:
            try:
                datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                return True
            except ValueError:
                return False
            except TypeError:
                return False

        if 'start_date' in self.__dict__.keys():
            start_date = self.start_date
        
        if start_date == 'default' or start_date is None:
            start_date = ''
        
        if start_date != '' and not is_valid_datetime(start_date):
            raise StartDateValueException("Start date is not in the correct format. It should be a valid date.")
        
        return datetime.datetime.strftime(dateutil.parser.parse(start_date), "%Y-%m-%d") if start_date != '' else ''
    
    def get_growth_model_object(self) -> Any:
        """
        Create and return the appropriate growth model object based on model type.

        Returns:
            Any: Instantiated growth model object

        Raises:
            ClassNotFoundException: If growth model class cannot be found
        """
        modules = self.get_modules()
        model_type = deepcopy(self.model_type)
        model_type = model_type.replace('_', ' ')
        model_type = model_type.split(' ')
        model_type = [w.capitalize() for w in model_type]
        model_type = ''.join(model_type)
        growth_model_classname = f"{model_type}GrowthModel"
        print(f"growth_model_classname={growth_model_classname}")

        for m in modules:
            if hasattr(m, growth_model_classname):
                cls = getattr(m, growth_model_classname)
                obj = cls(
                    data_storage=self.data_storage,
                    shared_memory=self.shared_memory, 
                    data_source=self.data_source,
                    parent=self.label,
                    colture_data={
                        'commodity_id': self.commodity_id,
                        'destination_use_id': self.destination_use_id,
                        'precocity_id': self.precocity_id,
                        'stage'       : os.getenv('dev_environment', None),
                        'model_type'  : self.model_type,
                        'start_date'  : self.get_start_date(),
                        'calendar_id' : self.calendar_id,
                        'policy_type_id': self.policy_type_id,
                        'planting_id'  : self.planting_id
                    })
                print(f'[Colture]: Oggetto creato della classe: {growth_model_classname}.')
                return obj
        raise ClassNotFoundException(f"Class: {growth_model_classname} cannot be found in modules available.")
    
    def run(self) -> None:
        """
        Execute the component's main functionality.

        Runs the growth model and susceptibility calculations if configured.
        Growth model is skipped if model_type is 'skip'.

        Raises:
            NotImplementedError: If data_source is 'local'
        """
        super().run()

        if self.data_source == 'local':
            raise NotImplementedError("local data_source not implemented yet.")
        
        # Run GrowthModel SubComponent
        if self.model_type != 'skip':
            growth_model_obj = self.get_growth_model_object()
            growth_model_data = growth_model_obj.run()

        # Run Susceptibility SubComponent
        if self.susceptibility is not None:
            self.susceptibility = Susceptibility(
                commodity_id=self.commodity_id,
                disease_id=self.susceptibility['kwargs']['disease_id'],
                variety_id=self.susceptibility['kwargs']['variety_id'])
            self.susceptibility.set_storage(self.data_storage)
            self.susceptibility.set_memory(self.shared_memory)
            self.susceptibility.run()

        print("[Colture]: Closed.")
        return

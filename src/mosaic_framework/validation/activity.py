################################################################################
# Module:      activity.py
# Description: Collection of activities, applied to a validation process, applied by
#              Validator.
# Author:      Stefano Zimmitti
# Date:        28/06/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import pandas as pd
import importlib
import pkgutil
import json

from mosaic_framework.config.configuration import MODEL_VALIDATION_ACTIVITY
from mosaic_framework.components.components import InternalComponent
from mosaic_framework.validation.exceptions import ValidationActivityDataFormatException
from mosaic_framework.engine.exceptions import ClassNotFoundException
import mosaic_framework

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.data_storage.resource import Resource
    from mosaic_framework.validation.model_validation import SimpleModelValidator
    
    MosaicDataStorageType    = MosaicDataStorage
    MosaicSharedMemoryType   = MosaicSharedMemory
    ResourceType             = Resource
    SimpleModelValidatorType = SimpleModelValidator

class ValidationActivity(InternalComponent):
    """
    ---\n
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=MODEL_VALIDATION_ACTIVITY, **kwargs)
        self.tag              = 'postprocess'
        self.data_storage     : MosaicDataStorageType   = None
        self.shared_memory    : MosaicSharedMemoryType  = None
        self.model_data       = None 
        self.validation_data  = None 
        self.report_flag      = None
    
    def set_model_data(self, value):
        self.model_data      = value
    
    def set_validation_data(self, value):
        self.validation_data = value
    
    def set_report_flag(self, value):
        self.report_flag = value

    def prepare(self)->None:
        """
        ---\n
        params:
        ---\n
        returns: 
        """

        super().prepare()

        #here we need to check, before running the Component, if
        #   model_data and validation_data has been set up

        if isinstance(self.model_data, dict):
            for df_k in list(self.model_data.keys()):
                if self.model_data[df_k].empty:
                    raise ValidationActivityDataFormatException("model_data has not properly been setup.")
        else:
            raise ValidationActivityDataFormatException("model_data is not a valid Dataframe.")

        #here we need to check, before running the Component, if
        #   model_data and validation_data has been set up
        if isinstance(self.validation_data, pd.DataFrame):
            if self.validation_data.empty:
                raise ValidationActivityDataFormatException("validation_data has not properly been setup.")
        else:
            raise ValidationActivityDataFormatException("validation_data is not a valid Dataframe.")

        return True
    
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

        return
    
class ModelValidation(ValidationActivity):
    """
    ---\n
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def get_model_validation_cls(model_validation_type:str):
        def get_model_validation_modules()->list:
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

        def get_class(modules: List[object], model_validation_type: str) -> ValidationActivity:
            """
            Allow to match the objects found in module (class_calls) with any module
            find in modules. Then it returns a list of objects that match, otherwise
            a ClassNotFoundException is raised.
            """
            cls = None
            obj_found = False
            for m in modules:
                if hasattr(m, model_validation_type):
                    cls = getattr(m, model_validation_type)
                    obj_found = True
            if not obj_found:
                raise ClassNotFoundException(f"Class: {model_validation_type} cannot be found in modules available.")
            return cls

        model_validation_cls_modules = get_model_validation_modules()
        model_validation_cls         = get_class(modules=model_validation_cls_modules, model_validation_type=model_validation_type)
        return model_validation_cls

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
        #self.data_model      = {'results': pd.DataFrame, 'compact_results': pd.DataFrame}
        #self.validation_data = pd.DataFrame

        #Chose the right class in 'model_validation' and launch it
        model_validation_type                      = f'{str(self.process).capitalize()}ModelValidator'
        ModelValidation : SimpleModelValidatorType = self.get_model_validation_cls(model_validation_type=model_validation_type)
        model_validation_obj = ModelValidation(
            model_data=self.model_data, 
            validation_data=self.validation_data, 
            data_storage=self.data_storage,
            shared_memory=self.shared_memory, 
            skip_report=not self.report_flag)
        validation_result = model_validation_obj.run()

        print("[ModelValidation] Closed running.")
        return validation_result

# TrialValidation (Not Yet developed or designed)
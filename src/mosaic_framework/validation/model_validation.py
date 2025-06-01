################################################################################
# Module:      model_validation.py
# Description: Collection of validations operations, 
#              that validate a dataset against another one,
#              based on what process has been chosen.
# Author:      Stefano Zimmitti
# Date:        04/07/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import pandas as pd

from mosaic_framework.components.sub_component import SubComponent
from mosaic_framework.validation.atomic_validation import IsColumnInplace, IsContentEqual
from mosaic_framework.config.validation_literals import RAISE_ERROR, RAISE_WARNING, SKIP, VALID
from mosaic_framework.validation.replace_policies import NanReplacePolicy
from mosaic_framework.validation.exceptions import ValidationProcessException

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.data_storage.resource import Resource
    
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory
    ResourceType           = Resource

class SimpleModelValidator(SubComponent):
    def __init__(self, skip_report:bool, model_data:dict, validation_data:pd.DataFrame, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType, **kwargs) -> None:
        super().__init__(**kwargs)
        self.skip_report    = skip_report
        self.data_storage   = data_storage
        self.shared_memory  = shared_memory
        self.results_model_data         = model_data.get("results", None)
        self.compact_results_model_data = model_data.get("compact_results", None)
        self.validation_data : pd.DataFrame           = validation_data
    
    def run(self) -> pd.DataFrame:
        #retrieve data
        output_labels = self.shared_memory.get_variable(key='outputs_labels', error_policy='raise').content

        print("[SimpleModelValidator] Started running...")
        #SimpleModelValidation is a collection of AtomicValidation:
        #   - Check if columns relative to output(s) are inplace in 'compact_results_model_data'
        #   - Check if columns relative to output(s) are inplace in 'validation_data'
        #   - for each column marked as output(s) (aka output of model)
        #       . Compare columns, with same name, in 'validation_data' and 'compact_results_model_data'
        replace_policies= [
            NanReplacePolicy(is_active=True, replace_fnc='nan_to_zero')
        ]
        
        validation_processes= [
            IsColumnInplace(
                ground=self.validation_data.columns.to_list(), 
                data=output_labels, 
                error_policy=RAISE_ERROR)
        ]
        #here we are sure that we have validated columns names, 
        #so we can get the real validation
        for inf_label in output_labels:
            validation_processes.append(
                IsContentEqual(
                    ground=self.validation_data[inf_label].to_list(), 
                    data=self.compact_results_model_data[inf_label].to_list(), 
                    error_policy=RAISE_WARNING,
                    replace_policy_collection=replace_policies))
        
        validation_results = list()
        for vp in validation_processes:
            result = vp.validate()
            if result['result'] == RAISE_ERROR:
                if self.skip_report:
                    raise ValidationProcessException(f"Validation process: {result['validation_process']} FAILED with error: {result['details']}")
                else:
                    validation_results.append(result)
            if result['result'] == RAISE_WARNING or result['result'] == VALID:
                validation_results.append(result)
        
        print("[SimpleModelValidator] Completed running...")

        return pd.DataFrame(data=validation_results)

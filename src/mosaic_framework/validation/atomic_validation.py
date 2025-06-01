################################################################################
# Module:      atomic_validation.py
# Description: Collection of validators, that validate a single unit of data.
#              Like Columns.
# Author:      Stefano Zimmitti
# Date:        28/06/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from mosaic_framework.validation.replace_policies import ReplacePolicy

from mosaic_framework.config.validation_literals import RAISE_ERROR, VALID

from collections.abc import Iterable
import pandas as pd
import json

class ColumnAtomicValidationRule():
    def __init__(self, data:object, ground:object, error_policy:str, replace_policy_collection:List[ReplacePolicy]=[]) -> None:
        self.data                          = data
        self.ground                        = ground
        self.error_policy                  = error_policy
        self.replace_policy_collection     = replace_policy_collection
    
    def replace_values(self)->None:
        #replace policy is applied to both ground and data
        for replace_policy in self.replace_policy_collection:
            self.data   = replace_policy.apply(self.data)
            self.ground = replace_policy.apply(self.ground)
        return
    
    def validate(self):
        #applying replace policies
        self.replace_values()
        return
    
class IsColumnInplace(ColumnAtomicValidationRule):
    def __init__(self, **kwargs) -> None:
        super().__init__(data=kwargs.get("data", None), ground=kwargs.get("ground", None), error_policy=kwargs.get("error_policy", RAISE_ERROR))
        self.formatted_cls_name = str(self.__class__)[str(self.__class__).rfind(".")+1:].replace("'>", '')
    
    def validate(self):
        super().validate()
        if not isinstance(self.data, Iterable):
            self.data = [self.data]

        if not isinstance(self.ground, list):
            raise ValueError("ground has the wrong format, must be a list.")
        
        result = all([d in self.ground for d in self.data])

        return {
            'validation_process' : self.formatted_cls_name,
            'result': VALID   if result else self.error_policy,
            "details": "None" if result else f"{self.data} differs from {self.ground}"
        }

class IsContentEqual(ColumnAtomicValidationRule):
    def __init__(self, **kwargs) -> None:
        super().__init__(data=kwargs.get("data", None), ground=kwargs.get("ground", None), error_policy=kwargs.get("error_policy", RAISE_ERROR), replace_policy_collection=kwargs.get("replace_policy_collection", RAISE_ERROR))
        self.formatted_cls_name = str(self.__class__)[str(self.__class__).rfind(".")+1:].replace("'>", '')
    
    def validate(self):
        self.replace_values()
        
        if not isinstance(self.data, Iterable):
            self.data = [self.data]

        if not isinstance(self.ground, list):
            raise ValueError("ground has the wrong format, must be a list.")
        
        msg = "data and ground differs."
        if len(self.data) == len(self.ground):
            result = all([self.data[i]==self.ground[i] for i, _ in enumerate(self.data)])
        else:
            result = False
            msg    = f"data and ground have different length ({len(self.data)}), ({len(self.ground)})"

        return {
            'validation_process' : self.formatted_cls_name,
            'result' : VALID   if result else self.error_policy,
            "details": "None"  if result else msg
        }


################################################################################
# Module:      replace_policies.py
# Description: Collection of replace policies operations, 
#              in order to replace values got in validation phase,
# Author:      Stefano Zimmitti
# Date:        16/07/2024
# Company:     xFarm Technologies
################################################################################

import numpy as np

from mosaic_framework.validation.exceptions import ReplacePolicyFunctionException

class ReplacePolicy():
    def __init__(self) -> None:
        pass

    def nan_to_zero(data:list):
        return [v if str(v).lower()!='nan' else 0.0 for v in data]
    
    def get_warning_msg(self):
        return str(self.__class__).replace("'>", "")[str(self.__class__).replace("'>", "").rfind('.')+1:] + " has been automatically applied during validation phase."

class NanReplacePolicy(ReplacePolicy):
    def __init__(self, is_active:bool=False, replace_fnc:str="nan_to_zero"):
        self.is_active   = is_active
        self.replace_fnc = replace_fnc
        self.warning_msg = self.get_warning_msg()
    
    def apply(self, data:list):
        #get the actual function to use, to replace values, 
        #defined in the superclass
        fnc = ReplacePolicy.__dict__.get(self.replace_fnc, None)
        if fnc == None:
            raise ReplacePolicyFunctionException(f"{self.replace_fnc} has not been implemented in ReplacePolicy superclass.")
        
        print(f"[Validation] {self.warning_msg}")
        return fnc(data)

################################################################################
# Module:      rules_hub.py
# Description: Defining a common place to save informations, useful for Rules.
# Author:      Stefano Zimmitti
# Date:        27/05/2024
# Company:     xFarm Technologies
################################################################################

from warnings import warn
from typing import List, Any
import pandas as pd
from copy import deepcopy

from mosaic_framework.core.environment.rules_env_variable import RulesEnvironmentVariable
from mosaic_framework.core.environment.exceptions import RulesEnvironmentVariableOverwrittenException, \
    RulesEnvironmentVariableNotFoundException

class MosaicRulesHub():
    """
    Centralized memory where non-structured informations are gathered. In order to have a unique
    entry point for fast-retrieved info, always having a centrilized memory storage. 
    Component can refers to it during runtime, it has no persistent layer to back it up, so
    when the elaboration is closed, the structure disappear. 
    """
    def __init__(self, config:dict) -> None:
        self.config     = config
        self.content    = dict()
        self.rule_imgs  = list()
    
    def add_variable(self, key:str, content:object, is_immutable:bool=False)->bool:
        """
        Add a RulesEnvironmentVariable to the shared memory, append it to the actual 'content'.
        RulesEnvironmentVariable is defined on demand inside the scope of this function.
        ---\n
        params:
        None
        ---\n
        returns:  
        (bool)-> Value returned based on adding result.
        """
        rules_environment_var = RulesEnvironmentVariable(
            key=key,
            content=content,
            is_immutable=is_immutable)
        if not rules_environment_var.key in list(self.content.keys()):
            self.content[rules_environment_var.key] = rules_environment_var
        else:
            raise RulesEnvironmentVariableOverwrittenException(f"You are trying to setup a new RulesEnvironmentVariable with an existing key: {rules_environment_var.key} ")
            
        print(f"\n[MosaicRulesHub]: {rules_environment_var} added to the MosaicRulesHub.")
        return rules_environment_var.key in list(self.content.keys())
    
    def get_variable(self, key:str, default:Any=None, original:bool=True, error_policy:str='pass')->RulesEnvironmentVariable:
        """
        Get a RulesEnvironmentVariable, based on label requested. You can specify also if you wanna 
        get the original version or not.
        ---\n
        params:
        - label:str, specify the information you want to get from the memory.
        - original:bool, wether or not you want the original form.
        ---\n
        returns:  
        (Resource)-> Get the requested RulesEnvironmentVariable if it is present.
        """
        #print(f"[MosaicRulesHub]: Resource will be searched: {key}")
        if key in list(self.content.keys()): 
            return {'key':key, 'content': self.content[key]} if not original else self.content[key]
        if error_policy=='raise':
            raise RulesEnvironmentVariableNotFoundException(f"Cannot get RulesEnvironmentVariable '{key}'. ")
        return {'key':key, 'content':default} if not original else default
    
    def update_variable(self, key:str, new_content:object)-> RulesEnvironmentVariable:
        """
        Update a RulesEnvironmentVariable, based on key requested, with new content specified
        .
        ---\n
        params:
        - key:str, specify the information you want to update from the memory.
        - new_content:object, specify the new content you want to replace.
        ---\n
        returns:  
        (RulesEnvironmentVariable)-> Get the new RulesEnvironmentVariable if it is present, and it's
        mutable.
        """

        for k in list(self.content.keys()):
            if k == key:
                self.content[k].update(new_content=new_content)
        return self.content[k]
    
    def register(self, rule:Any)->None:
        """Register a single rule into the MosaicRulesHub.

        Args:
            rule (Any): Rule to be added.
        """
        filtered_rule = dict()
        for p in self.config.get("params_to_include", []):
            if p in list(rule.__dict__.keys()):
                filtered_rule[p] = rule.__dict__.get(p)
        
        if filtered_rule: 
            self.rule_imgs.append(filtered_rule)
        else:
            print(f"[MosaicRulesHub] cannot be added.")
        return 
    
    def remove_implicit_columns(self, data:pd.DataFrame)->pd.DataFrame:
        """Remove all the implicit columns that have not been removed in the previous steps.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: updated dataframe.
        """
        updt_df = deepcopy(data)

        for ri in self.rule_imgs:
            if ri.get("column") in updt_df.columns and ri.get("is_implicit")==True:
                updt_df.drop(ri.get("column"), axis=1, inplace=True)      
        return updt_df
    
    def __str__(self)-> str:
        data_str = ""
        for k, v in self.content.items():
            data_str += f"\n[{v}]"
        return f"\n--------------------------------\n"+"\MosaicRulesHub content:\n"+data_str+f"\n--------------------------------"
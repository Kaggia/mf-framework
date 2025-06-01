################################################################################
# Module:      shared_memory.py
# Description: Defining a common place to save informations, useful for components.
# Author:      Stefano Zimmitti
# Date:        27/05/2024
# Company:     xFarm Technologies
################################################################################

from warnings import warn
from typing import List
import datetime
import tempfile
import shutil
import os

from mosaic_framework.data_storage.variable import SharedVariable
from mosaic_framework.data_storage.exceptions import SharedVariableOverwrittenException, SharedVariableNotFoundException

class MosaicSharedMemory():
    """
    Centralized memory where non-structured informations are gathered. In order to have a unique
    entry point for fast-retrieved info, always having a centrilized memory storage. 
    Component can refers to it during runtime, it has no persistent layer to back it up, so
    when the elaboration is closed, the structure disappear. 
    """
    def __init__(self, DEBUG:bool) -> None:
        self.DEBUG      = DEBUG
        self.content    = dict()
    
    def add_variable(self, key:str, content:object, is_immutable:bool=False)->bool:
        """
        Add a SharedVariable to the shared memory, append it to the actual 'content'.
        SharedVariable is defined on demand inside the scope of this function.
        ---\n
        params:
        None
        ---\n
        returns:  
        (bool)-> Value returned based on adding result.
        """
        shared_variable = SharedVariable(
            key=key,
            content=content,
            is_immutable=is_immutable)
        if not shared_variable.key in list(self.content.keys()):
            self.content[shared_variable.key] = shared_variable
        else:
            raise SharedVariableOverwrittenException(f"You are trying to setup a new SharedVariable with an existing key: {shared_variable.key} ")
            
        print(f"\n[MosaicSharedMemory]: {shared_variable} added to the MosaicSharedMemory.")
        return shared_variable.key in list(self.content.keys())
    
    def get_variable(self, key:str, original:bool=True, error_policy:str='pass')->SharedVariable:
        """
        Get a SharedVariable, based on label requested. You can specify also if you wanna 
        get the original version or not.
        ---\n
        params:
        - label:str, specify the information you want to get from the memory.
        - original:bool, wether or not you want the original form.
        ---\n
        returns:  
        (Resource)-> Get the requested SharedVariablee if it is present.
        """
        print(f"[MosaicSharedMemory]: Resource will be searched: {key}")
        if key in list(self.content.keys()): 
            return {'key':key, 'content': self.content[key]} if not original \
                   else self.content[key]
        if error_policy=='raise':
            raise SharedVariableNotFoundException(f"Cannot get SharedVariable '{key}'. ")
        return None
    
    def update_variable(self, key:str, new_content:object)-> SharedVariable:
        """
        Update a SharedVariable, based on key requested, with new content specified
        .
        ---\n
        params:
        - key:str, specify the information you want to update from the memory.
        - new_content:object, specify the new content you want to replace.
        ---\n
        returns:  
        (SharedVariable)-> Get the new SharedVariable if it is present, and it's
        mutable.
        """

        for k in list(self.content.keys()):
            if k == key:
                self.content[k].update(new_content=new_content)
        return self.content[k]
    
    def __str__(self)-> str:
        data_str = ""
        for k, v in self.content.items():
            data_str += f"\n[{v}]"
        return f"\n--------------------------------\n"+"\nMosaicSharedMemory content:\n"+data_str+f"\n--------------------------------"
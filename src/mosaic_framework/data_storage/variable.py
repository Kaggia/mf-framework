################################################################################
# Module:      resource.py
# Description: Defining a common place to gather data and configuration.
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################

import re
import inspect

from mosaic_framework.data_storage.exceptions import ImmutableVariableUpdateException

class SharedVariable():
    """
    It rapresents a single dictionary in the MosaicSharedMemory. In order to get it when it is needed.
    It has also a param to describe its immutability
    """

    def __init__(self, key:str, content:object, is_immutable:bool=False) -> None:
        self.key           = key
        self.content       = content 
        self.is_immutable  = is_immutable
        self.shared_memory = None
    
    def update(self, new_content:object):
        """
        Look for a single SharedVariable, and try to update the content. It returns the 
        updated version of it, if it successes otherwise returns an error.
        ---\n
        params:
        - key (str): key that is look for in the MosaicSharedMemory, in order to the content
        be updated.
        - content (object) : New content to be set.
        ---\n
        returns: 
        Processor : Contains the Processor that have the tag specified.
        """

        if not self.is_immutable:
            self.content = new_content
        else:
            raise ImmutableVariableUpdateException(f"Cannot update the SharedVariable ({self.key}) cause it's immutable.")

        return self
    
    def __str__(self):
        return f"SharedVariable:: key={self.key} | content={self.content} | type={type(self.content)} | {'mutable' if not self.is_immutable else 'immutable'}"
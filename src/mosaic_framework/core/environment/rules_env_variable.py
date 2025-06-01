################################################################################
# Module:      rules_env_variable.py
# Description: Defining a common place to gather data and configuration for rules.
# Author:      Stefano Zimmitti
# Date:        29/10/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.core.environment.exceptions import ImmutableRulesEnvironmentVariableUpdateException

class RulesEnvironmentVariable():
    """
    It rapresents a single dictionary in the MosaicRulesHub. In order to get it when it is needed.
    It has also a param to describe its immutability
    """

    def __init__(self, key:str, content:object, is_immutable:bool=False) -> None:
        self.key           = key
        self.content       = content 
        self.is_immutable  = is_immutable
        self.rules_hub     = None
    
    def update(self, new_content:object):
        """
        Look for a single RulesEnvironmentVariable, and try to update the content. It returns the 
        updated version of it, if it successes otherwise returns an error.
        ---\n
        params:
        - key (str): key that is look for in the MosaicRulesHub, in order to the content
        be updated.
        - content (object) : New content to be set.
        ---\n
        returns: 
        RulesEnvironmentVariable : Contains the data as RulesEnvironmentVariable.
        """

        if not self.is_immutable:
            self.content = new_content
        else:
            raise ImmutableRulesEnvironmentVariableUpdateException(f"Cannot update the RulesEnvironmentVariable ({self.key}) cause it's immutable.")

        return self
    
    def __str__(self):
        return f"RulesEnvironmentVariable:: key={self.key} | content={str(self.content)} | {'mutable' if not self.is_immutable else 'immutable'}"
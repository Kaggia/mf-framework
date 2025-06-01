################################################################################
# Module:      exceptions.py
# Description: Exceptions of Model module.
# Author:      Stefano Zimmitti
# Date:        27/05/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.exceptions import ComponentException

class ModelException(ComponentException):
    """Exception raised"""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class OutputRulesMissingException(ComponentException):
    """Exception raised when rules are not presente for an output"""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataFormatException(ComponentException):
    """Exception raised when data got as input do not have the correct format."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class RulesFormatError(ComponentException):
    """Exception raised when output rules list is empty for a selected output."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

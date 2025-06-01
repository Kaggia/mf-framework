################################################################################
# Module:      exceptions.py
# Description: Exceptions of Model module.
# Author:      Stefano Zimmitti
# Date:        27/05/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.exceptions import ComponentException

class AgronomicsException(ComponentException):
    """Exception raised from the agronomics module"""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataFormatException(AgronomicsException):
    """Exception raised when data got as input do not have the correct format."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class StartDateValueException(AgronomicsException):
    """Exception raised when date got as input do not have the correct format."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


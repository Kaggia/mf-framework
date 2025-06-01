################################################################################
# Module:      exceptions.py
# Description: Exceptions of Validation.
# Author:      Stefano Zimmitti
# Date:        03/07/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.exceptions import ComponentException

class ValidationException(ComponentException):
    """Exception raised in Validation processing.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ValidationActivityDataFormatException(ComponentException):
    """Exception raised when data has not been set up properly for
    a validation activity.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ValidationProcessException(ComponentException):
    """Exception raised when a validation atomic process is produced.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ReplacePolicyFunctionException(ComponentException):
    """Exception raised when a replace function has not been implemented.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class InputValidationException(ComponentException):
    """Exception raised when input validation fails.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataFillingPolicyException(ComponentException):
    """Exception raised when input validation fails.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


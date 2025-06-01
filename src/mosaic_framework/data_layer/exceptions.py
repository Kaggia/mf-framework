################################################################################
# Module:      exceptions.py
# Description: Exceptions for data layer.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

class DataLayerException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class ParameterNotAllowedException(DataLayerException):
    """Exception raised when a parameter passed is not allowed.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class APIPermissionException(DataLayerException):
    """Exception raised when a API returns 503 error.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class APIServiceInternalException(DataLayerException):
    """Exception raised when a API returns 503 error.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class MappingEngineException(DataLayerException):
    """Exception raised when it is not found the mapping engine.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
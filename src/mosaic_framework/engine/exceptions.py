################################################################################
# Module:      exception.py
# Description: Exceptions of Engine.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

class EngineException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class ClassNotFoundException(EngineException):
    """Exception raised when a class found in custom module is not found in 
       MosaicFramework package.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class AssigningComponentException(EngineException):
    """Exception raised when a Component cannot be assigned, by a tag, to a Processor."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class UniqueComponentException(EngineException):
    """Exception raised when a Component cannot be assigned, by a tag, to a Processor."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


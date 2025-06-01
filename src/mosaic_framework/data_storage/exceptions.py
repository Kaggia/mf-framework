################################################################################
# Module:      exception.py
# Description: Exceptions of DataStorage.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

class DataStorageException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class ResourceNotFoundException(DataStorageException):
    """Exception raised when trying to get a SharedVariable, but not found."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ImmutableVariableUpdateException(DataStorageException):
    """Exception raised when you try to update the content of an immutable SharedVariable."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class SharedVariableOverwrittenException(DataStorageException):
    """Exception raised when trying to setup a new SharedVariable with an existing key."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class SharedVariableNotFoundException(DataStorageException):
    """Exception raised when trying to get a SharedVariable, but not found."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class WriterClassNotFoundException(DataStorageException):
    """Exception raised when trying to get a non-existing or implemented writer."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ConvertionException(DataStorageException):
    """Exception raised when configurations about a converter cannot be found."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataFormatException(DataStorageException):
    """Exception raised when read data have not the format expected."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)




################################################################################
# Module:      exceptions.py
# Description: Collections of exceptions relative to datetime parsing.
# Author:      Stefano Zimmitti
# Date:        07/10/2024
# Company:     xFarm Technologies
################################################################################

class DataStorageException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class DateParsingException(DataStorageException):
    """Exception raised when trying to get a SharedVariable, but not found."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

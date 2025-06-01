################################################################################
# Module:      exception.py
# Description: Exceptions of Engine.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.exceptions import ComponentException

class SourceNotRecognizedException(ComponentException):
    """Exception raised when a not recognized environment is set as 
    'environment' parameter in Source Component.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class FolderNotFoundException(ComponentException):
    """Exception raised when folder has not been created by the analyst.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class LocalFolderCannotBeCreatedException(ComponentException):
    """Exception raised when folder has not been in the local or cloud (local) environment.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ColumnsParamNotValidException(ComponentException):
    """Exception raised when folder has not been in the local or cloud (local) environment.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DuplicateMappingColumnsException(ComponentException):
    """Exception raised when folder has not been in the local or cloud (local) environment.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

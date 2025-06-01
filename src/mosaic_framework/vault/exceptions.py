################################################################################
# Module:      exceptions.py
# Description: 
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

class VaultException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class APIKeySecretRetrievingException(VaultException):
    """Exception raised when a secret cannot be correctly retrieved.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class SecretNotMappedException(VaultException):
    """Exception raised when a secret name cannot be correctly mapped.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

################################################################################
# Module:      exception.py
# Description: Exceptions of MosaicRulesHub.
# Author:      Stefano Zimmitti
# Date:        29/10/2024
# Company:     xFarm Technologies
################################################################################

class MosaicRulesHubException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class ImmutableRulesEnvironmentVariableUpdateException(MosaicRulesHubException):
    """Exception raised when you try to update the content of an immutable RulesEnvironmentVariable."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class RulesEnvironmentVariableOverwrittenException(MosaicRulesHubException):
    """Exception raised when trying to setup a new RulesEnvironmentVariable with an existing key."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class RulesEnvironmentVariableNotFoundException(MosaicRulesHubException):
    """Exception raised when trying to get a RulesEnvironmentVariable, but not found."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)



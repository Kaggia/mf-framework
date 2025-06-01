################################################################################
# Module: exceptions.py
# Description: Exceptions raised in the tool.
# Author: Stefano Zimmitti
# Date: 08/08/2024
# Company: xFarmTech
################################################################################

class ToolException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class PipConfigurationContentException(ToolException):
    """Exception raised when a the content of pip.conf has not been properly updated.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

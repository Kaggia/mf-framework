################################################################################
# Module:      exceptions.py
# Description: Exceptions for components.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

class ComponentException(Exception):
    """Base class for exceptions in the mosaic_framework, engine sub-package."""
    pass

class ComponentParameterException(ComponentException):
    """Exception raised when a mandatory param is missing, after been checked 
    with configuration of that Component.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

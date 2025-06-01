################################################################################
# Module:      exceptions.py
# Description: Exceptions of Retrieving.
# Author:      Stefano Zimmitti
# Date:        31/05/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.exceptions import ComponentException

class RetrievingException(ComponentException):
    """Exception raised when a not recognized environment is set as 
    'environment' parameter in Source Component.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class InvalidConnectionException(ComponentException):
    """Exception raised when a 2 to 2 connection is tried to be set
    .
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataBridgeConnectionException(ComponentException):
    """Exception raised when you are trying to define a new Connector when 
    labels in the 'components_image' shared variable are defined badly, or 
    generally cannot be found any label in 'components_image' that matches 
    connect_in or connect_out.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

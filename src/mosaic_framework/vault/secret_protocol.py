################################################################################
# Module:      secret_protocol.py
# Description: Protocol for each class that implements functions of secret 
#              retrieving.
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

from typing import Protocol

class ProtocolSecret(Protocol):        
    def get(self):
        ...

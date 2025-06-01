################################################################################
# Module:      protocol_component.py
# Description: Protocol for Component.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

from typing import Protocol

class ProtocolComponent(Protocol):
    def validate_input(self):
        ...
    
    def prepare(self):
        ...
    
    def run(self):
        ...

class ProtocolSubComponent(Protocol):    
    def run(self):
        ...
################################################################################
# Module:      protocol_processor.py
# Description: Protocol for Processor.
# Author:      Stefano Zimmitti
# Date:        20/05/2024
# Company:     xFarm Technologies
################################################################################

from typing import Protocol

from mosaic_framework.components.components import Component

class ProtocolProcessor(Protocol):
    def add_component(self, component:Component):
        ...
    
    def run(self):
        ...
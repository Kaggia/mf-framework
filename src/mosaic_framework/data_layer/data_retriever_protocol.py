################################################################################
# Module:      data_retriever_protocol.py
# Description: Protocol for each class that implements functions of data
#              retrieving.
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

from typing import Protocol

class ProtocolDataRetriever(Protocol):    
    def build(self):
        ...
    
    def retrieve(self):
        ...

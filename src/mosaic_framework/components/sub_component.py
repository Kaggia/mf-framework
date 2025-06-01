################################################################################
# Module:      sub_components.py
# Description: Sub components defined at runtime.
# Author:      Stefano Zimmitti
# Date:        31/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

from mosaic_framework.components.protocol_component import ProtocolSubComponent

class SubComponent(ProtocolSubComponent):
    """
    SubComponent is the basic class that describes objects that are produced during runtime,
    but are directly consequence of a Component definition (or running), so they are 
    instanciated during runtime.
    For example Connector is an object that can't be written in a agro file, but is istanciated
    at runtime.
    ---\n
    Available functions:
    """
    def __init__(self, parent:str=None, **kwargs) -> None:
        self.parent = parent
################################################################################
# Module:      source.py
# Description: Handle the geospatial data source.
# Author:      Stefano Zimmitti
# Date:        26/08/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import os
import pandas as pd
from typing import Any
import warnings

from mosaic_framework.config.configuration import GEOSPATIAL_SOURCE
from mosaic_framework.components.components import Component

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

class GeospatialSource(Component):
    """GeospatialSource is a component that handles location data.

    Args:
        label (str): name of the component
        latitude (float): latitude of the point that is going to be used in the data retrieving.
        longitude (float): longitude of the point that is going to be used in the data retrieving.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=GEOSPATIAL_SOURCE, **kwargs)
        self.tag            = 'preprocess'
        self.data_storage : MosaicDataStorageType  = None
        self.shared_memory: MosaicSharedMemoryType = None
        self.is_unique      = True
        self.exec_priority  = 8
    
    def run(self)->None:
        """
        Main entry point of the component. It loads the geospatial data into the MosaicSharedMemory.\n
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        super().run()
        
        #Load into MosaicSharedMemory a new variable, merging latitude and longitude
        self.shared_memory.add_variable(
            key='geospatial_data',
            content={'latitude':self.latitude, "longitude":self.longitude},
            is_immutable=True)
        
        print("[GeospatialSource]: Location dumped into MosaicSharedMemory.")
        print("[GeospatialSource] Closed running.")
        return
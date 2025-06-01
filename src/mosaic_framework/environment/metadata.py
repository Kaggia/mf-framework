################################################################################
# Module:      metadata.py
# Description: Handle metadata relative to the Model | Pipeline.
# Author:      Stefano Zimmitti
# Date:        24/07/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import TYPE_CHECKING

from mosaic_framework.config.configuration import METADATA
from mosaic_framework.components.components import Component

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

class Metadata(Component):
    """
    This class is responsible for handling metadata relative to the Model | Pipeline.

    Args:
        :param kwargs: Additional keyword arguments for the Component class.
    ---\n
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=METADATA, **kwargs)
        self.tag          = 'preprocess'
        self.data_storage : MosaicDataStorageType  = None
        self.shared_memory: MosaicSharedMemoryType = None
    
    def run(self)->None:
        """
        Run the Metadata component. Excludes variables from the metadata dictionary, 
        then stores the remaining metadata in the shared memory.
        ---\n
        returns: None
        """
        super().run()

        exclude_vars = ['tag', 'data_storage', 'shared_memory', 'label']

        filtered_metadata = {k: v for k, v in self.__dict__.items() if k not in exclude_vars}
        
        self.shared_memory.add_variable(key='metadata', content=filtered_metadata, is_immutable=False)

        print("[Metadata]: Closed running.")
        return
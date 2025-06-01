################################################################################
# Module:      exception.py
# Description: Module used to handle with all processing about preparsing, a
#              MosaicPipeline .py file, before Component parsing.
# Author:      Stefano Zimmitti
# Date:        28/08/2024
# Company:     xFarm Technologies
################################################################################
from __future__ import annotations
from typing import List, TYPE_CHECKING

from copy import deepcopy
import os

from mosaic_framework.data_storage.resource import Resource

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory


class RawParser():
    def __init__(self, filepath:str, params:dict, prefix:str=None, ) -> None:
        self.prefix       = prefix
        self.filepath     = filepath
        self.params       = params
        self.data_storage : MosaicDataStorageType = None

    def set_storage(self, data_storage:MosaicDataStorage):
        self.data_storage = data_storage

    def parse(self)->str:
        """
        Entry point of Class, it runs the entire file parsing, replacing each one
        of the param found in params, returning the name of the Resource that has
        been written in the DataStorage.
        """
        #Open filepath with read

        file_path = self.filepath \
            if self.prefix==None \
            else self.prefix + "/" + self.filepath
        
        print(f"[Raw Parser] model file is retrieved from: {file_path}")

        label = "parsed_model"

        #converting all params to strings
        for param_key in self.params.keys():
            self.params[param_key] = str(self.params[param_key])

        #Apply the replacing
        with open(file_path, 'r') as file:
            raw_content = deepcopy(file.readlines())
            #For each param found in the params, we apply the replace
            for param_key, param_value in self.params.items():
                for i, line in enumerate(raw_content):
                    if '$'+param_key in line:
                        print(f"[Raw Parser] replacing '${param_key}' with '{param_value}'")
                    raw_content[i] = line.replace('$'+param_key, param_value)
            raw_content = "".join(raw_content)

        print(f"[Raw Parser] MosaicPipeline raw file has been updated: \nparams:\n{self.params}\ncontent:\n{raw_content}")

        #Drop the file into MosaicDataStorage
        self.data_storage.add_resource(resource=Resource(label=label, data=raw_content, file_type="py"))
        return label
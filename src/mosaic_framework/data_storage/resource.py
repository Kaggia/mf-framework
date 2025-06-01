################################################################################
# Module:      resource.py
# Description: Defining a common place to gather data and configuration.
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################

import os
import re
import inspect

from mosaic_framework.config.configuration import WRITERS_MAPPING, READERS_MAPPING
from mosaic_framework.data_storage.exceptions import WriterClassNotFoundException
import mosaic_framework.data_storage.readers
import mosaic_framework.data_storage.writers

class Resource():
    """
    Represents a single file in the MosaicDataStorage. Allows retrieving the file when needed
    .
    """

    def __init__(self, label: str, data: object, file_type: str, description: str = "") -> None:
        """
        Initialize the Resource with a label, data, file type, and an optional description
        .
        ---\n
        params:
        label: str - The label of the resource.
        data: object - The data to be stored in the resource.
        file_type: str - The type of file (e.g., 'txt', 'json', 'csv', 'xlsx').
        description: str - An optional description of the resource.
        ---\n
        returns: None
        """
        self.label        = label
        self.data         = data 
        self.file_type    = file_type
        self.description  = description
        self.data_storage = None
    
    def persist(self, data_storage) -> bool:
        """
        Implements the persistence of a Resource in the MosaicDataStorage. Finds the correct writer in the writers module,
        then uses it to write the file in the MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object where the resource will be persisted.
        ---\n
        returns: 
        bool - Value returned based on persistence result.
        """
        def find_cls(module, cls_string):
            # Inspects all classes defined in the module
            for name, cls in inspect.getmembers(module, inspect.isclass):
                # Gets the source code of the class
                source = inspect.getsource(cls)
                # Searches for the string in the source code of the class
                if re.search(cls_string, source):
                    return cls
            raise WriterClassNotFoundException(f"Cannot find a proper writer for the selected string: {cls_string}")

        # Based on file type we choose a Writer to let the file persist in MosaicDataStorage
        writer_str = WRITERS_MAPPING[self.file_type]
        writer_obj = find_cls(module=mosaic_framework.data_storage.writers, cls_string=writer_str)(data_storage=data_storage)
        ack = writer_obj.persist(label=self.label, data=self.data)
        self.data_storage = data_storage
        return ack
    
    def remove(self) -> bool:
        """
        Remove the Resource from the MosaicDataStorage.
        ---\n
        params:
        None
        ---\n
        returns:
        bool - Value returned based on the result of removing the resource.
        """
        #remove file
        filepath = self.data_storage.path + "/" + self.label + "." +self.file_type
        os.remove(filepath)
        return True
    
    def get_data(self):
        """
        Get the data contained in the resource, handling single or multi-line data
        .
        ---\n
        params:
        None
        ---\n
        returns: 
        object - Value(s) returned based on the content of the resource.
        """
        def find_cls(module, cls_string):
            # Inspects all classes defined in the module
            for name, cls in inspect.getmembers(module, inspect.isclass):
                # Gets the source code of the class
                source = inspect.getsource(cls)
                # Searches for the string in the source code of the class
                if re.search(cls_string, source):
                    return cls

        reader_str = READERS_MAPPING[self.file_type]
        reader_obj = find_cls(module=mosaic_framework.data_storage.readers, cls_string=reader_str)(data_storage=self.data_storage)
        data = reader_obj.read(label=self.label)
        return data

    def __str__(self):
        return f"Resource:: {self.label} | {self.file_type} | '{self.description}'"

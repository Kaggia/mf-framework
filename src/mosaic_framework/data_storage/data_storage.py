################################################################################
# Module:      data_storage.py
# Description: Defining a common place to gather data and configuration.
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################
from __future__ import annotations
from typing import List, TYPE_CHECKING
from warnings import warn
from typing import List
import datetime
import tempfile
import shutil
import os

if TYPE_CHECKING:
    from mosaic_framework.data_storage.resource import Resource
    ResourceType           = Resource   

from mosaic_framework.data_storage.exceptions import ResourceNotFoundException

class MosaicDataStorage():
    """
    Centralized storage where all data and settings are gathered. In order to have a unique
    entry point for data, always knowing where they are stored. Components can refer to a local or cloud resource, 
    then we transfer data to the MosaicDataStorage and always have control over this kind of object, 
    and use it during the elaboration
    .
    """
    def __init__(self, DEBUG:bool) -> None:
        """
        Initialize the MosaicDataStorage object with the provided debug setting.
        ---\n
        params:
        DEBUG: bool - Enable or disable debug mode.
        ---\n
        returns: None
        """
        self.DEBUG      = DEBUG
        self.prefix     = "mosaic"
        self.path       = None
        self.content    = []
    
    def allocate(self):
        """
        Simply allocate a temp folder with a prefix of 'mosaic' and suffix
        equal to the current datetime.
        ---\n
        params:
        None
        ---\n
        returns:  
        str - The path of the allocated temporary directory.
        """
        print("\n[MosaicDataStorage]: allocating space...\n")
        temp_dir  = tempfile.mkdtemp(
            prefix=self.prefix+"_",
            suffix="_"+datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M"))
        self.path = temp_dir
        print(f"\n[MosaicDataStorage]: allocated in: {temp_dir}\n")
        return temp_dir
    
    def deallocate(self) -> bool:
        """
        In order to keep a clean environment, it is necessary to deallocate all resources loaded
        into the MosaicDataStorage, cleaning 'path' and 'content'. If it is not possible, 
        False is returned; otherwise, True is returned.
        ---\n
        params:
        None
        ---\n
        returns:  
        bool - Value returned based on the deallocation result.
        """
        try:
            shutil.rmtree(self.path)
            
            if os.path.isdir(self.path):
                warn("MosaicDataStorage has not been correctly deallocated")
            else:
                print(f"\n[MosaicDataStorage]: data deallocated.")
                self.path    = None
                self.content = []
            return True
        except:
            print(f"\nCannot deallocate MosaicDataStorage: {self.path}.")
            return False

    def add_resource(self, resource: ResourceType) -> bool:
        """
        Add a Resource to the storage, append it to the current 'content'
        .
        ---\n
        params:
        resource: Resource - The resource to be added to the storage.
        ---\n
        returns:  
        bool - Value returned based on the result of adding the resource.
        """
        self.content.append(resource)
        is_added = resource.persist(data_storage=self)
        print(f"\n[MosaicDataStorage]: {resource} added to the MosaicDataStorage.")
        return is_added
    
    def remove_resource(self, label:str) -> bool:
        """
        Remove a Resource from the storage.
        ---\n
        params:
        label: str - label of the resource to be removed from the storage.
        ---\n
        returns:
        bool - Value returned based on the result of removing the resource.
        """
        for r in self.content:
            if r.label == label:
                #remove the resource 'physical' part
                r.remove()
                #remove the resource 'logical' part
                self.content.remove(r)
                print(f"\n[MosaicDataStorage]: {r} removed from the MosaicDataStorage.")
                return True
        raise ResourceNotFoundException(f"Cannot removed Resource. Cannot find resource by label='{label}'")
    
    def get_resource(self, label: str, error_policy: str = 'pass') -> ResourceType:
        """
        Get a resource based on the requested label.
        ---\n
        params:
        label: str - The label of the resource to be retrieved.
        error_policy: str - The policy for handling errors ('pass' or 'raise'). Default is 'pass'.
        ---\n
        returns:  
        Resource - The requested resource if it is present, otherwise None.
        """
        print(f"[MosaicDataStorage]: Resource will be searched: {label}")
        for r in self.content:
            if r.label == label:
                print(f"[MosaicDataStorage]: Resource {r} found.")
                return r
        if error_policy == 'raise':
            raise ResourceNotFoundException(f"Cannot find resource by label='{label}'")
        return None
    
    def replace_resource(self, new_resource:Resource) -> bool:
        """
        Update a Resource in the storage. 

        Args:
            new_resource: Resource - The new resource to be added to the storage.

        Returns:
            bool - Value returned based on the result of updating the resource.        
        """
        print(f"[MosaicDataStorage]: Resource will be updated: {new_resource.label}")

        #remove the old version of the Resource
        self.remove_resource(label=new_resource.label)
        self.add_resource(resource=new_resource)
    
        return 
    
    def __str__(self):
        """
        Return a string representation of the MosaicDataStorage content.
        ---\n
        params:
        None
        ---\n
        returns:
        str - String representation of the MosaicDataStorage content.
        """
        return f"\n--------------------------------\nMosaicDataStorage content:\n"+str('\n '.join([str(r) for r in self.content])+"\n--------------------------------")

################################################################################
# Module:      connector.py
# Description: Implements the effective connection between components.
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

from mosaic_framework.components.sub_component import SubComponent
from mosaic_framework.data_storage.variable import SharedVariable

class Connector(SubComponent):
    """
    This component maps a connection between two Components. Connecting two
    components in a relation 1 to 1. 
    .
    ---\n
    Available params:
    - label(str):                   Define the name of the Connector, useful when you want
                                    to refer to a particular Connector, during the elaboration flow. 
    If left empty a random string is created.
    - label_in (str): This is the start of connection, it is expressed as a single string
    - label_out (str): This is the end of connection, it is expressed as a single string
    - tag(str):                     tag the processor where this component will be assigned.
    ---\n
    Examples:\n
    - Connector(label_in='source_name', label_out='agro_model'), which grant a
    connection between a Source and a Model component.
    """

    def __init__(self, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType) -> None:
        self.data_storage  = data_storage
        self.shared_memory = shared_memory
    
    def run(self)->None:
        """
        Entry function of the connector. This at runtime will estabilish which 
        connection will be instancied, based on which Component is connecting.
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """

        super().run()
        
        return
    
    def __str__(self):
        return f'{str(type(self))[str(type(self)).rfind(".")+1:]}:{self.__dict__}'.replace("'>", "")

class SourceToModelConnector(Connector):
    """
    This component maps a connection between a Source and a Model
    . 
    ---\n
    Available params:
    - connect_in  (str): This is the start of connection, it is expressed as a single string
    - connect_out (str): This is the end of connection, it is expressed as a single string
    ---\n
    Examples:\n
    - SourceToModelConnector(source_in='source_name', model_out='agro_model'), which grant a
    connection between a Source and a Model component.
    """

    def __init__(self, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType, connect_in:str, connect_out:str) -> None:
        super().__init__(data_storage=data_storage, shared_memory=shared_memory)
        self.connect_in  = connect_in
        self.connect_out = connect_out
    
    def run(self):
        """
        Get connectors from SharedMemory, if they exist: create a new key inside 'connectors',
        otherwise: create 'connectors' as list, append {'connect_in': <str>, 'connect_out': <str>, 'resource': <Resource>}
        .
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """
        print("[SourceToModelConnector] Start running...")
        
        existing_connectors = self.shared_memory.get_variable(key="connectors")
        if existing_connectors != None:
            #existing case, getting old content, creating new connector and appending.
            new_content : list   = self.shared_memory.get_variable(key="connectors").content
            new_connector        = {
                        'connect_in':  self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource':    self.data_storage.get_resource(label=self.connect_in)}
            new_content.append(new_connector)
            self.shared_memory.update_variable(key='connectors', new_content=new_content)
        else:
            #not existing case
            self.shared_memory.add_variable(
                key='connectors', 
                content=[
                    {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : self.data_storage.get_resource(label=self.connect_in)}])
        print("[SourceToModelConnector] Closed running...")
        return

class SourceToValidatorConnector(Connector):
    """
    This component maps a connection between a Source and a Validator
    . 
    ---\n
    Available params:
    - connect_in  (str): This is the start of connection, it is expressed as a single string
    - connect_out (str): This is the end of connection, it is expressed as a single string
    ---\n
    Examples:\n
    - SourceToValidatorConnector(connect_in='source_label', connect_out='validator_label'), which grant a
    connection between a Source and a Validator component.
    """

    def __init__(self, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType, connect_in:str, connect_out:str) -> None:
        super().__init__(data_storage=data_storage, shared_memory=shared_memory)
        self.connect_in  = connect_in
        self.connect_out = connect_out
    
    def run(self):
        """
        Get connectors from SharedMemory, if they exist: create a new key inside 'connectors',
        otherwise: create 'connectors' as list, append {'connect_in': <str>, 'connect_out': <str>, 'resource': <Resource>}
        .
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """
        print("[SourceToValidatorConnector] Start running...")
        
        existing_connectors = self.shared_memory.get_variable(key="connectors")
        if existing_connectors != None:
            #existing case, getting old content, creating new connector and appending.
            new_content : list   = self.shared_memory.get_variable(key="connectors").content
            new_connector        = {
                        'connect_in': self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource': self.data_storage.get_resource(label=self.connect_in)}
            new_content.append(new_connector)
            self.shared_memory.update_variable(key='connectors', new_content=new_content)
        else:
            #not existing case
            self.shared_memory.add_variable(
                key='connectors', 
                content=[
                    {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : self.data_storage.get_resource(label=self.connect_in)}])
        print("[SourceToValidatorConnector] Closed running...")
        return

class ColtureToModelConnector(Connector):
    """
    This component maps a connection between a Colture and a Model
    . 
    ---\n
    Available params:
    - connect_in  (str): This is the start of connection, it is expressed as a single string
    - connect_out (str): This is the end of connection, it is expressed as a single string
    ---\n
    Examples:\n
    - ColtureToModelConnector(source_in='source_name', model_out='agro_model'), which grant a
    connection between a Colture and a Model component.
    """

    def __init__(self, data_storage:MosaicDataStorageType, shared_memory:MosaicSharedMemoryType, connect_in:str, connect_out:str) -> None:
        super().__init__(data_storage=data_storage, shared_memory=shared_memory)
        self.connect_in  = connect_in
        self.connect_out = connect_out
    
    def run(self):
        """
        Get connectors from SharedMemory, if they exist: create a new key inside 'connectors',
        otherwise: create 'connectors' as list, append {'connect_in': <str>, 'connect_out': <str>, 'resource': <Resource>}
        .
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """
        print("[SourceToModelConnector] Start running...")
        
        existing_connectors = self.shared_memory.get_variable(key="connectors")
        if existing_connectors != None:
            #existing case, getting old content, creating new connector and appending.
            new_content : list   = self.shared_memory.get_variable(key="connectors").content
            new_connector        = {
                        'connect_in':  self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource':    self.data_storage.get_resource(label=self.connect_in)}
            new_content.append(new_connector)
            self.shared_memory.update_variable(key='connectors', new_content=new_content)
        else:
            #not existing case
            self.shared_memory.add_variable(
                key='connectors', 
                content=[
                    {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : self.data_storage.get_resource(label=self.connect_in)}])
        print("[SourceToModelConnector] Closed running...")
        return

class ModelToValidatorConnector(Connector):
    """
    This component maps a connection between a Model and a Validator
    . 
    ---\n
    Available params:
    - source_label (str): This is the start of connection, it is expressed as a single string
    - model_out (str): This is the end of connection, it is expressed as a single string
    ---\n
    Examples:\n
    - SourceToModelConnector(source_in='source_name', model_out='agro_model'), which grant a
    connection between a Source and a Model component.
    """

    def __init__(self, data_storage: MosaicDataStorage, shared_memory: MosaicSharedMemory, connect_in:str, connect_out:str) -> None:
        super().__init__(data_storage, shared_memory)
        self.connect_in  = connect_in
        self.connect_out = connect_out

    def run(self):
        """
        Get connectors from SharedMemory, if they exist: create a new key inside 'connectors',
        otherwise: create 'connectors' as list, append {'connect_in': <str>, 'connect_out': <str>, 'resource': <AllocatedResource>}
        .
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """
        print("[ModelToValidatorConnector] Start running...")

        #We do not already know the Model result (input of connector)
        #So we need to allocate an empty SharedVariable, to fill 
        #afterwards when Model will calculate.
        pre_allocated_resource = SharedVariable(key=self.connect_in,content=None, is_immutable=False)
        
        existing_connectors = self.shared_memory.get_variable(key="connectors")
        if existing_connectors != None:
            #existing case, getting old content, creating new connector and appending.
            new_content : list   = self.shared_memory.get_variable(key="connectors").content
            
            new_connector        = {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : pre_allocated_resource}
            new_content.append(new_connector)
            self.shared_memory.update_variable(key='connectors', new_content=new_content)
        else:
            #not existing case
            self.shared_memory.add_variable(
                key='connectors', 
                content=[
                    {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : pre_allocated_resource}])
        print("[ModelToValidatorConnector] Closed running...")
        return

class ModelToLambdaOutputConnector(Connector):
    """
    This component maps a connection between a Model and a LambdaOutput
    . 
    ---\n
    Available params:
    - source_label (str): This is the start of connection, it is expressed as a single string
    - model_out (str): This is the end of connection, it is expressed as a single string
    ---\n
    Examples:\n
    - ModelToLambdaOutputConnector(source_in='source_name', model_out='agro_model'), which grant a
    connection between a Source and a Model component.
    """

    def __init__(self, data_storage: MosaicDataStorage, shared_memory: MosaicSharedMemory, connect_in:str, connect_out:str) -> None:
        super().__init__(data_storage, shared_memory)
        self.connect_in  = connect_in
        self.connect_out = connect_out

    def run(self):
        """
        Get connectors from SharedMemory, if they exist: create a new key inside 'connectors',
        otherwise: create 'connectors' as list, append {'connect_in': <str>, 'connect_out': <str>, 'resource': <AllocatedResource>}
        .
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """
        print("[ModelToLambdaOutputConnector] Start running...")

        #We do not already know the Model result (input of connector)
        #So we need to allocate an empty SharedVariable, to fill 
        #afterwards when Model will calculate.
        pre_allocated_resource = SharedVariable(key=self.connect_in,content=None, is_immutable=False)
        
        existing_connectors = self.shared_memory.get_variable(key="connectors")
        if existing_connectors != None:
            #existing case, getting old content, creating new connector and appending.
            new_content : list   = self.shared_memory.get_variable(key="connectors").content
            
            new_connector        = {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : pre_allocated_resource}
            new_content.append(new_connector)
            self.shared_memory.update_variable(key='connectors', new_content=new_content)
        else:
            #not existing case
            self.shared_memory.add_variable(
                key='connectors', 
                content=[
                    {
                        'connect_in' : self.connect_in, 
                        'connect_out': self.connect_out, 
                        'resource'   : pre_allocated_resource}])
        print("[ModelToValidatorConnector] Closed running...")
        return

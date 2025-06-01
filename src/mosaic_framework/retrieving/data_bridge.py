################################################################################
# Module:      data_bridge.py
# Description: Connect two Components, in order to share something, usually data.
# Author:      Stefano Zimmitti
# Date:        31/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING
import pkgutil
import importlib

import mosaic_framework.retrieving
import mosaic_framework.retrieving.connectors
from mosaic_framework.engine.exceptions import ClassNotFoundException
from mosaic_framework.retrieving.exceptions import DataBridgeConnectionException

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.retrieving.connectors import Connector
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory
    ConnectorType          = Connector

import mosaic_framework
from mosaic_framework.components.components import Component
from mosaic_framework.config.configuration import DATA_BRIDGE
from mosaic_framework.retrieving.exceptions import InvalidConnectionException

class DataBridge(Component):
    """
    This component maps a connection between two Components. Based on which component
    is connected a Connector object is instanciated and data exchanged accordingly to
    the behaviour of the Connector. You are allowed to specify connect_in or connect_out
    with more Components (list of them), but be care cause only one of them can be a List of
    component's label, otherwise an Exception is raised. Connections generally must be:
    - 1 to 1 
    - 1 to 2
    - 2 to 1 (to be revised)\n
    But each of one of the case is mapped as multiple 1 to 1
    .
    ---\n
    Available params:
    - label(str):                   Define the name of the databridge, useful when you want
                                    to refer to a particulare databridge, during the elaboration flow. 
    If left empty a random string is created.
    - connect_in (str)|(List[str]): This is the start of connection, it is expressed as a single string
                                    when you want to connect a single Component, or a List of strings,
                                    when you want to connect multiple Component.
    - connect_out (str)|(List[str]): This is the end of connection, it is expressed as a single string
                                    when you want to connect a single Component, or a List of strings,
                                    when you want to connect multiple Component. 
    - tag(str):                     tag the processor where this component will be assigned.
    ---\n
    Examples:\n
    - DataBridge(connect_in='source_name', connect_out='agro_model'), which grant a
    connection between a Source and a Model component.
    - DataBridge(connect_in='agro_model', connect_out='agro_model_validator'), which grant a
    connection between a Model and a Validator component.\n
    - DataBridge(connect_in='source_name', connect_out=['agro_model', 'agro_model_2']), which grant a
    connection between a Source and a more Model components.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=DATA_BRIDGE, **kwargs)
        self.tag          = 'data'
        self.exec_priority= 0
        self.data_storage : MosaicDataStorageType
        self.shared_memory: MosaicSharedMemoryType
    
    @staticmethod
    def get_modules()->list:
        """
        Run over mosaic_framework.retrieving.connectors package, find any Connector that is part of the package
        then add them to a list. Modules are found dynamically at runtime.
        ---\n
        params:
        None
        ---
        returns: List of modules find in mosaic_framework.retrieving.connectors path.
        """
        # Scorri tutti i moduli nel package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.retrieving.__path__, mosaic_framework.retrieving.__name__ + '.'):
            # Importa il modulo dinamicamente
            module = importlib.import_module(module_name)
            # Aggiungi il modulo alla lista
            modules.append(module)

        return modules
    
    def get_one_to_one(self, connect_in:str, connect_out:str)->Connector:
        """
        Based on connect_in and connect_out that are label, look for the actual Components 
        and get the correct Connector. Data are looked in 'components_image' in MosaicSharedMemory
        .
        ---\n
        params:
        None
        ---
        returns (Connector) Get the right Connector, based on connect_in and connect_out.
        """

        # Based on connect_in and connect_out that are label, 
        # look for the actual Components and get the correct
        # Connector

        #Need a reference to the current Components
        try:

            connect_in_classname  = self.shared_memory.get_variable(key='components_image').content[connect_in]
            connect_out_classname = self.shared_memory.get_variable(key='components_image').content[connect_out]
        except KeyError as ke:
            raise DataBridgeConnectionException(f"Cannot get proper connection between '{connect_in}' and '{connect_out}'. Maybe labels have been defined wrongly.") from ke
        
        class_name = connect_in_classname + "To" + connect_out_classname + "Connector"
        print(f"[DataBridge] {class_name} class will be used to map connection.")
        modules   = self.get_modules()
        for m in modules:
            if hasattr(m, class_name):
                cls = getattr(m, class_name)
                obj = cls(
                    data_storage=self.data_storage, 
                    shared_memory=self.shared_memory,
                    connect_in=connect_in, 
                    connect_out=connect_out) 
                print(f'Oggetto creato della classe: {class_name}.')
                return obj
        raise ClassNotFoundException(f"Class: {class_name} cannot be found in modules available.")

    def get_connectors(self)-> List[Connector]:
        """
        Based on what types of relation there's between connected Components get a different list of
        of Connectors
        .
        ---\n
        params:
        None
        ---
        returns (List[Connector]) Get the right Connectors, based on connect_in and connect_out.
        """

        connectors = list()

        #We have to define each one of the possibilities:
        # [1 to 1, 1 to 2, 2 to 1] reducing to 1 to 1
        is_connect_in_iterable  = isinstance(self.connect_in,  list)
        is_connect_out_iterable = isinstance(self.connect_out, list)

        if is_connect_in_iterable and is_connect_out_iterable:
            raise InvalidConnectionException(f"Cannot estabilish connections between Components. Too many connections.")
        
        if is_connect_in_iterable:
            for c in self.connect_in:
                connectors.append(self.get_one_to_one(c.connect_in, self.connect_out))
        elif is_connect_out_iterable:
            for c in self.connect_out:
                connectors.append(self.get_one_to_one(self.connect_in, c.connect_out))
        else:
            connectors.append(self.get_one_to_one(self.connect_in, self.connect_out))
        return connectors
    
    def run(self)->None:
        """
        Entry function of the component. This at runtime will estabilish which 
        connector will be instancied, based on which Component is connecting.
        So it has a dynamic behaviour.
        ---\n
        params:
        None.
        ---\n
        returns:  
        None
        """

        super().run()

        #This will contain all connectors
        connectors = self.get_connectors()
        for c in connectors:
            c.run()

        print("[DataBridge] Closed running.")
        return
    
    def __str__(self):
        return f'{str(type(self))[str(type(self)).rfind(".")+1:]}:{self.__dict__}'.replace("'>", "")

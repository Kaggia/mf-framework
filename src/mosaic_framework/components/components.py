################################################################################
# Module:      components.py
# Description: Basic components, used to map code written by analyst.
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.components.protocol_component import ProtocolComponent
from mosaic_framework.components.exceptions import ComponentParameterException
import mosaic_framework.components.fill_parameter_method 

import inspect
import re

class Component(ProtocolComponent):
    """
    Component is the basic class that describes objects that are expressed in a python
    module written by an analyst (a developer or an agronomist).
    ---\n
    Available functions:
    validate_input\n
    auto_map_input\n
    auto_assign_input\n
    load_component_params\n
    set_storage\n
    set_memory\n
    prepare\n
    run\n
    __str__\n
    """
    def __init__(self, configuration:dict, **kwargs) -> None:
        """
        Initialize the Component object with the provided configuration and parameters.
        ---\n
        params:
        configuration: dict - Configuration dictionary for the component.
        kwargs: Additional parameters passed to the component.
        ---\n
        returns: None
        """
        self.config         = configuration
        self.params         = kwargs
        self.is_unique      = False
        self.exec_priority  = 9
        self.data_storage   = None
        self.shared_memory  = None
        self.tag            = None
        self.validate_input()
        self.auto_map_input()
        self.auto_assign_input()
        self.load_component_params()

    def validate_input(self)->bool:
        """
        Validate each parameter found in the component definition, checks are implemented 
        as guards. If checks are fulfilled, True is returned.
        ---\n
        params:
        None
        ---\n
        returns: 
        bool - True if all checks are passed, otherwise an error is raised.
        """
        #pointing to 'params'
        params_config   = self.config['params']
        #pointing to 'metadata' | extra infos about parameters and class
        metadata_config = self.config['metadata']

        #Here we are checking if a param in a component definition
        #has each one of the non-optional params.
        for param_key, param_val in params_config.items():
            if not param_key in self.params.keys() and params_config[param_key]['optional']==False: 
                raise ComponentParameterException(f"'{param_key}' misses from Component mandatory configuration.")                
        
        #Here we need to check if each value of the param associated is in 
        #the list of possible values
        for param_key, param_val in self.params.items():
            if param_key in list(params_config.keys()):
                if not param_val in params_config[param_key]['values'] and not "Any" in params_config[param_key]['values']:
                    raise ComponentParameterException(f"'{param_val}' is not accepted as valid parameter value. Accepted are: {params_config[param_key]['values']}")                
            else:
                if metadata_config['allowed-extra-params'] == False:
                    raise ComponentParameterException(f"'{param_key}' is not accepted as valid parameter value. Extra params are not allowed for this component.")                
                #this is an extra param, must be dealt with.
        return True

    def auto_map_input(self)->None:
        """
        Run over the params, checking whether the value is 'default' or not, 
        if it is, then update it with a fixed mapping. This mapping can be found in
        configuration.py under configuration folder in mosaic_framework.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        #pointing to 'params'
        params_config   = self.config['params']
        #pointing to 'metadata' | extra infos about parameters and class
        metadata_config = self.config['metadata']

        for param_key, param_val in self.params.items():
            if param_val == "default":
                self.__dict__[param_key] = params_config[param_key]['mapping']['default']
        return
    
    def auto_assign_input(self)->None:
        """
        Run over the params, if they are not present then assign 
        the value of default.
        ---\n
        params:
        None
        ---\n
        returns:  
        None
        """
        #Find function in fill_parameter_method.py
        def find_fnc(module, fnc_string):
            # Ispeziona tutte le funzioni definite nel modulo
            for name, func in inspect.getmembers(module, inspect.isfunction):
                # Ottieni il codice sorgente della funzione
                source = inspect.getsource(func)
                # Cerca la stringa nel codice sorgente della funzione
                if re.search(fnc_string, source):
                    return func

        #pointing to 'params'
        params_config   = self.config['params']
        #pointing to 'metadata' | extra infos about parameters and class
        metadata_config = self.config['metadata']

        for param_key in list(params_config.keys()):
            if not param_key in list(self.params.keys()):
                if "Function::" in str(params_config[param_key]['default']):
                    method_name = params_config[param_key]['default'].replace("Function::", "")
                    self.__dict__[param_key] = \
                        find_fnc(mosaic_framework.components.fill_parameter_method, method_name)()
                else:
                    self.__dict__[param_key] = params_config[param_key]['default']
        return 
    
    def load_component_params(self)->object:
        """
        At this step, we have objects params separated in 'params' and outside params. 
        Ultimately, we load each (key, val) in params, then pop the 'params' key.
        ---\n
        params:
        None
        ---\n
        returns:  
        object - The popped 'params' dictionary or an error string if it fails.
        """
        #pointing to 'params'
        params_config   = self.config['params']
        #pointing to 'metadata' | extra infos about parameters and class
        metadata_config = self.config['metadata']

        for key, value in self.params.items():
            self.__dict__[key] = value
        
        params_popped = self.__dict__.pop('params', "POP_PARAMS_ERROR")
        return params_popped
    
    def set_storage(self, data_storage:MosaicDataStorage)->None:
        """
        Set the current MosaicDataStorage, granting operations on and from it.
        ---\n
        params:
        data_storage: MosaicDataStorage - Current MosaicDataStorage used for elaborations.
        ---\n
        returns:  
        None
        """

        self.data_storage = data_storage
        return

    def set_memory(self, shared_memory:MosaicSharedMemory)->None:
        """
        Set the current MosaicSharedMemory, granting operations on and from it.
        ---\n
        params:
        shared_memory: MosaicSharedMemory - Current MosaicSharedMemory used for elaborations.
        ---\n
        returns:  
        None
        """

        self.shared_memory = shared_memory
        return
    
    def prepare(self):
        """
        Common code for preparing the Component. Used before starting the effective running.
        ---\n
        params:
        None
        ---\n
        returns:  
        None
        """

        return
    
    def run(self)->None:
        """
        Common code for running the Component.
        ---\n
        params:
        None
        ---\n
        returns:  
        None
        """

        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Started elaboration (run).")
        return
    
    def __str__(self):
        """
        Return a string representation of the Component object.
        ---\n
        params:
        None
        ---\n
        returns:
        str - String representation of the Component object.
        """
        return f'{str(type(self))[str(type(self)).rfind(".")+1:]}:{self.__dict__}'.replace("'>", "")

class InternalComponent(Component):
    """
    InternalComponent is a subclass of Component designed to handle internal components.
    """
    def __init__(self, configuration: dict, **kwargs) -> None:
        """
        Initialize the InternalComponent object with the provided configuration and parameters.
        ---\n
        params:
        configuration: dict - Configuration dictionary for the internal component.
        kwargs: Additional parameters passed to the internal component.
        ---\n
        returns: None
        """
        super().__init__(configuration, **kwargs)
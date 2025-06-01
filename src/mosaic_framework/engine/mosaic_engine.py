################################################################################
# Module:      exception.py
# Description: Entry point of Mosaic elaboration.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
import time
import os
import pkg_resources
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from mosaic_framework.engine.processor import Processor

from mosaic_framework.config.local_configuration import MOSAIC_FRAMEWORK_LOCAL_PATHS
from mosaic_framework.engine.component_parser import ComponentParser
from mosaic_framework.engine.preparsing import RawParser
from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.components.components import Component
from mosaic_framework.engine.exceptions import AssigningComponentException
from mosaic_framework.engine.processor import (PreProcessor, 
    DataProcessor, ModelProcessor, PostProcessor)


class MosaicEngine():
    """
    This is the entire entry point of the Mosaic processing. In order it reads a 
    pseudo-Python module, get all the Compoents, classify and assign them to a 
    specific Processor, than each Processor runs. There are 4 main Processors: 
    - PreProcessor, runs each Component that need to be run before main processing.
    - DataProcessor, runs each Component connected to data.
    - ModelProcessor, runs each Component connected to modelling.
    - PostProcessor, runs each Component about validation and postmodelling operations.
    ---\n
    Available functions:
    - validate_input
    - auto_map_input
    - auto_assign_input
    - load_component_params
    - set_storage
    """

    def __init__(self, input_file:str, DEBUG:bool=False, **kwargs) -> None:
        self.DEBUG            = DEBUG
        self.input_file       = "models" + "/" + input_file
        self.cloud_tmp_fld    = kwargs.get('cloud_temp_folder', None)
        self.data_storage     = MosaicDataStorage(DEBUG=DEBUG)
        self.shared_memory    = MosaicSharedMemory(DEBUG=DEBUG)
        self.raw_parser       = RawParser(prefix=kwargs.get('cloud_temp_folder', None), filepath=self.input_file, params=kwargs.get('parsing_params', {}))
        self.component_parser = ComponentParser(DEBUG=DEBUG)
        self.processors       = list()

    @staticmethod
    def __get_title(env:str):
        """
        Returns the title of the Mosaic elaboration.
        ---\n
        params:
        - env (str): Environment where the Mosaic is running.
        ---\n
        Returns:
        - str : Title of the Mosaic elaboration.
        """
        if env == 'cloud':
            return f"\n\n-------------------------\nMosaicEngine started in cloud environment\nVersion:{pkg_resources.get_distribution('mosaic_framework').version}\n-------------------------\n"
        elif env == 'local':
            title = "\n\n\n" + "----------------------"*3
            title = title + """
╔╦╗┌─┐┌─┐┌─┐┬┌─┐  ╔═╗┬─┐┌─┐┌┬┐┌─┐┬ ┬┌─┐┬─┐┬┌─
║║║│ │└─┐├─┤││    ╠╣ ├┬┘├─┤│││├┤ ││││ │├┬┘├┴┐
╩ ╩└─┘└─┘┴ ┴┴└─┘  ╚  ┴└─┴ ┴┴ ┴└─┘└┴┘└─┘┴└─┴ ┴
"""
            title = title + f"\nVersion:{pkg_resources.get_distribution('mosaic_framework').version}\n{'----------------------'*3}\n"
            return title

    def get_processor(self, tag:str)-> Processor:
        """
        Returns a single processor, that matches the tag requested.
        ---\n
        params:
        - tag (str): Tag expressed, must match at least one Processor.tag, otherwise 
        None is returned.
        ---\n
        Returns:
        - Processor : Contains the Processor that have the tag specified.
        """

        for i, p in enumerate(self.processors):
            if p.tag == tag:
                return p[i]
        return None
    
    def add_processor(self, processor:Processor)-> List[Processor]:
        """
        Add a single Processor to the current list of processors, returning the updated
        version of it.
        ---\n
        params:
        - processor (Processor): Current Processor that is gonna be added
        ---\n
        Returns:
        - List[Processor] : Contains the updated list of processors.
        """

        self.processors.append(processor)
        return self.processors
    
    def assign_component(self, component:Component, processors:List[Processor])->List[Processor]:
        """
        Assign a component, with a specified tag, to the processor with that tag.
        ---\n
        params:
        - component:Component that is needed to be assigned to a Processor available
        - processors:List[Processor] are di available processors.
        ---\n
        Returns:
        - List[Processor] update list of processors, with components assigned.
        """
        for i, p in enumerate(processors):
            if p.tag == component.tag:
                processors[i].add_component(component=component)
                return processors
        
        raise AssigningComponentException(f"Cannot assign {component} with tag to any processor.")

    def run(self):
        """
        Entry point function of the whole processing.
        ---\n
        params:
        None
        ---\n
        Returns:
        - None
        """
        start_time = time.time()

        #Allocating space for MosaicDataStorage
        self.data_storage.allocate()

        self.data_storage.add_resource(
            resource=Resource(
                label="input_file_name", 
                file_type="txt", 
                data=(self.input_file \
                    if not '.py' in self.input_file \
                    else self.input_file[:self.input_file.find('.py')] ).replace("models/", "")))
        
        print(self.data_storage)

        #Loading to shared memory self.cloud_tmp_fld
        self.shared_memory.add_variable(key='cloud_tmp_fld', content=str(self.cloud_tmp_fld), is_immutable=True)

        #Printing the title of the Mosaic elaboration
        print(self.__get_title(env='local' if str(self.cloud_tmp_fld)=="None" else 'cloud'))
        
        print(self.shared_memory)

        #Preparse the agro_model file   - once parsed drop into MosaicDataStorage
        self.raw_parser.set_storage(data_storage=self.data_storage)
        resource_model_label = self.raw_parser.parse()

        #Parsing the agro_model file(s) - read it from MosaicDataStorage
        #than apply the abstract tree parsing and match each element with
        #a valid Component found. 
        self.component_parser.set_storage(data_storage=self.data_storage)
        objects  = self.component_parser.parse(model_label=resource_model_label)
        
        #debug stuff that need to be removed.
        print("\n")
        for o in objects:
            debug_o = o.__dict__
            try:
                del debug_o['config']
            except KeyError:
                pass
            print(f"{type(o)}::{debug_o}")
        #-------------------------------------
        
        #Inject useful object into Components.
        for o in objects:
            if isinstance(o, Component):
                o.set_storage(data_storage=self.data_storage)
                o.set_memory(shared_memory=self.shared_memory)

        #Set the processors:
        preprocessor     = PreProcessor(tag='preprocess', components=objects)
        data_processor   = DataProcessor(tag='data')
        model_processor  = ModelProcessor(tag='model')
        post_processor   = PostProcessor(tag='postprocess')

        self.add_processor(processor=preprocessor)
        self.add_processor(processor=data_processor)
        self.add_processor(processor=model_processor)
        self.add_processor(processor=post_processor)

        #Inject useful object into Processors.
        for p in self.processors:
            p.set_memory(shared_memory=self.shared_memory)

        #We run over each single one of the Components parsed and found.
        #We must run with an order and prio from max to min. 
        for component in objects:
            self.processors = self.assign_component(
                component=component, processors=self.processors)
        
        #Run each processor defined, they are launched by instanciation-time.
        print("\n")
        for p in self.processors:
            p.run()
        
        print(self.shared_memory)
        
        print(self.data_storage)

        end_time = time.time()
        #Deallocating MosaicDataStorage
        self.data_storage.deallocate()
        print(f"\n[MosaicEngine]: completed elaboration in: {round(end_time-start_time, 2)} seconds.\n")
        return
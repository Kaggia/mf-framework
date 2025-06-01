################################################################################
# Module:      processor.py
# Description: Collection of processors.
# Author:      Stefano Zimmitti
# Date:        20/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

from mosaic_framework.engine.protocol_processor import ProtocolProcessor
from mosaic_framework.components.components import InternalComponent
from mosaic_framework.engine.exceptions import UniqueComponentException

if TYPE_CHECKING:
    from mosaic_framework.components.components import Component
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicSharedMemoryType = MosaicSharedMemory

class Processor(ProtocolProcessor):
    """
    Processor is the basic class that handles a collection of Components. Each Processor
    has a different purpose and is launched at different moments.
    ---\n
    Available functions:\n
    - add_component\n
    - run
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize the Processor with optional keyword arguments.
        ---\n
        params:
        kwargs: Optional keyword arguments to configure the Processor.
        ---\n
        returns: None
        """
        self.tag = kwargs.get('tag', None)
        self.components = list()
        self.shared_memory: MosaicSharedMemoryType = None

    def add_component(self, component: Component) -> List[Component]:
        """
        Add a single component to the list of available components.
        ---\n
        params:
        component: Component - The component to be added.
        ---\n
        returns: 
        List[Component] - Updated list of available components.
        """
        self.components.append(component)
        print(f"\n[{str(type(self))[str(type(self)).rfind('.')+1:str(type(self)).rfind('>')-1]}]: added component-> {component}")
        return self.components

    def validate_components(self, components:List[Component]) -> List[str]:
        """
        Validate components before starting the elaboration. Launch a collection of
        functions to validate each Component. Raising exception on failed validations
        ---\n
        """
        def is_unique_validation(components:List[Component]):
            """
            Validate if a component is unique.
            """
            data = []
            for c in components:
                if type(c) in data and c.is_unique==True:
                    raise UniqueComponentException(f"Component {type(c)} should be unique.")
                else:
                    data.append(type(c))
            
            print("is_unique_validation-> ", data)
            return True
        """
        Check if some Component is unique, cause some of them must be unique.
        ---\n
        params:
        component_labels: dict - Labels and Classes for each parsed Component.
        ---\n
        returns:
        List[str] - List of unique labels.
        """
        #Launch all validations
        is_unique_validation(components=components)
        return True

    def set_memory(self, shared_memory: MosaicSharedMemoryType) -> None:
        """
        Set the current MosaicSharedMemory, enabling operations on and from it.
        ---\n
        params:
        shared_memory: MosaicSharedMemory - The current MosaicSharedMemory used for elaborations.
        ---\n
        returns:  
        None
        """
        self.shared_memory = shared_memory

    def run(self):
        """
        Run common code for each Processor.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Started elaboration (run).")
        validation_result= self.validate_components(components=self.components)

        #Sorting components by priority
        self.components.sort(key=lambda c: c.exec_priority, reverse=False)
        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Execution priorities: {[(c.label, c.exec_priority) for c in self.components]}.")

    def __str__(self):
        return '\n' + str(type(self))[str(type(self)).rfind('.')+1:str(type(self)).rfind('>')-1] + " contains: \n" + '| '.join([str(c) + '\n' + '***' * 20 + '\n' for c in self.components])

class PreProcessor(Processor):
    """
    PreProcessor designed to gather useful information before the main elaboration starts.
    Interacts mainly with the OS environment to set necessary data. Also ensures each 
    connection is alive, allowing each DataBridge assigned to this Processor to run.
    For example, setting the global environment (local | cloud) before running the Source component.
    ---\n
    Available functions:\n
    - add_component\n
    - run
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.parsed_components: Component = kwargs.get('components', None)
    
    def get_component_labels(self) -> dict:
        """
        Define a dictionary where each key is a label of a parsed Component.
        ---\n
        params:
        None
        ---\n
        returns:  
        dict - Labels as keys and Classes for each parsed Component.
        """
        return {pc.label: str(type(pc))[str(type(pc)).rfind('.')+1:-2] for pc in self.parsed_components}
    
    def load_component_images_to_memory(self, component_labels: dict) -> bool:
        """
        Add a SharedVariable to the MosaicSharedMemory, containing the component labels obtained
        with the 'get_component_labels' function of this class.
        ---\n
        params:
        component_labels: dict - Labels and Classes for each parsed Component.
        ---\n
        returns:  
        bool - True if the operation is successful.
        """
        self.shared_memory.add_variable(key="components_image", content=component_labels, is_immutable=False)
        return True

    def run(self):
        """
        Update shared memory with a list of actual parsed Components, 
        so that the correct Connectors can be instantiated in each DataBridge 
        present in the SharedMemory. Also run each component tagged as 'preprocess'.
        ---\n
        params:
        None
        ---\n
        returns:  
        None
        """
        super().run()

        # Update shared memory with a list of actual parsed Components
        component_labels = self.get_component_labels()
        response         = self.load_component_images_to_memory(component_labels=component_labels)
        
        # Run main function for each component
        for c in self.components:
            # Skip InternalComponents, which are defined in .py files but cannot run in a processor initially
            if not isinstance(c, InternalComponent):
                c.run()

        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Closed elaboration (run).")

class DataProcessor(Processor):
    """
    DataProcessor designed to handle data processing tasks.
    ---\n
    Available functions:\n
    - add_component\n
    - run
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    def run(self):
        """
        Run data processing tasks for each component.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        super().run()

        # Run main function for each component
        for c in self.components:
            # Skip InternalComponents
            if not isinstance(c, InternalComponent):
                c.run()

        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Closed elaboration (run).")

class ModelProcessor(Processor):
    """
    ModelProcessor designed to handle model-related processing tasks.
    ---\n
    Available functions:\n
    - add_component\n
    - run
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    def run(self):
        """
        Run model processing tasks for each component.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        super().run()
        
        # Run main function for each component
        for c in self.components:
            # Skip InternalComponents
            if not isinstance(c, InternalComponent):
                c.run()

        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Closed elaboration (run).")

class PostProcessor(Processor):
    """
    PostProcessor designed to handle tasks after the main processing.
    ---\n
    Available functions:\n
    - add_component\n
    - run
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    def run(self):
        """
        Run post-processing tasks for each component.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        super().run()
        
        # Run main function for each component
        for c in self.components:
            # Skip InternalComponents
            if not isinstance(c, InternalComponent):
                c.run()

        print(f"\n[{str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind('>')-1]}]: Closed elaboration (run).")

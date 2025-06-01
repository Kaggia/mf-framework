import inspect
import unittest
import os

from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.engine.component_parser import ComponentParser
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.environment.source import Source
class ComponentParserTest(unittest.TestCase):
    maxDiff = None  

    """
    Testing MapValues:
        test_0_single_component : MosaicPipeline with single component in it.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/ComponentParser/"
        self.data_storage = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        return 
    def tearDown(self) -> None:
        return

    def test_0_single_component(self):
        test_assertResult      = None
        #Load a MosaicPipeline into the MosaicDataStorage
        with open(self.data_folder +"data/"+ f"{str(inspect.currentframe().f_code.co_name)}.py", "r+") as test_data_f:
            test_data          = test_data_f.read()
            self.data_storage.add_resource(
                resource=Resource(
                    label="mosaic_pipeline_test_file",
                    data=test_data,
                    file_type="py"
                )
            )
        
        component_parser = ComponentParser()
        component_parser.set_storage(data_storage=self.data_storage)
        components       = component_parser.parse(model_label="mosaic_pipeline_test_file")
        
        #Checking the type of the result
        self.assertEqual(type(components), list)
        self.assertEqual(type(components[0]), Source)
        
        return
    
if __name__ == '__main__':
    unittest.main()


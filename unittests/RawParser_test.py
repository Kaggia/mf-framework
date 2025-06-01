import inspect
import unittest
import os

from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.engine.preparsing import RawParser
class TestRawParser(unittest.TestCase):
    maxDiff = None  

    """
    Testing MapValues:
        test_0_parsing_case   : MosaicPipeline with $variables and params.
        test_1_no_parsing_case: MosaicPipeline no   $variables and params.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/RawParser/"
        self.data_storage = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        return 
    def tearDown(self) -> None:
        return

    def test_0_parsing_case(self):
        test_assertResult = None

        with open(self.data_folder +"assertResult/"+ f"{str(inspect.currentframe().f_code.co_name)}.py", "r+") as test_data_f:
            test_assertResult  = test_data_f.read()
        
        print("test->", os.getcwd())
        
        raw_parser = RawParser(
            filepath=str(os.getcwd())+"/"+self.data_folder +"data/"+f"{str(inspect.currentframe().f_code.co_name)}.py",
            prefix="",
            params={'doyStart':121.0})
        
        raw_parser.set_storage(data_storage=self.data_storage)
        
        model_label   = raw_parser.parse()
        
        #Model content contains the parsed (eventually) model 
        # (MosaicPipeline) file
        model_content = self.data_storage.get_resource(
            label=model_label, 
            error_policy = 'raise').get_data()

        self.assertEqual(model_content, test_assertResult)        
        return
    
    def test_1_no_parsing_case(self):
        test_assertResult = None

        with open(self.data_folder +"assertResult/"+ f"{str(inspect.currentframe().f_code.co_name)}.py", "r+") as test_data_f:
            test_assertResult  = test_data_f.read()
        
        print(os.getcwd())

        raw_parser = RawParser(
            filepath=str(os.getcwd())+"/"+self.data_folder +"data/"+f"{str(inspect.currentframe().f_code.co_name)}.py",
            prefix="",
            params={})
        
        raw_parser.set_storage(data_storage=self.data_storage)

        model_label   = raw_parser.parse()
        
        #Model content contains the parsed (eventually) model 
        # (MosaicPipeline) file
        model_content = self.data_storage.get_resource(
            label=model_label, 
            error_policy = 'raise').get_data()

        self.assertEqual(model_content, test_assertResult)        
        return
    
if __name__ == '__main__':
    unittest.main()


import os
import json
import unittest
import shutil

from mosaic_framework.environment.source import Source
from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.environment.columns.columns import *

class TestSource(unittest.TestCase):
    """
    Testing MapValues:
        test_0: Entire processing of Source Component.
        test_1: Processing with columns equal to 'auto' (or not specified).
        test_2: Processing with columns specified.
    """

    def setUp(self) -> None:
        try:
            self.data_folder = "unittests/data/Source_input/"
            self.data_storage   = MosaicDataStorage(DEBUG=True)
            self.data_storage.allocate()
            self.shared_memory  = MosaicSharedMemory(DEBUG=True)

            with open(self.data_folder + f"data.json", "r+") as test_data_f, \
                 open(self.data_folder + f"assertResult.json", "r+") as assertResult_f:
                self.test_assertResult_file  = json.load(assertResult_f)['assertResult']
                os.mkdir("temp")
                os.mkdir("temp/data")
                temp_f = open("temp/data/data.json", "wb")
                temp_f.write(test_data_f.read().encode('utf-8'))
                os.chdir("temp/")
        except Exception as e:
            print(f"Error during setUp function: {e}")
            self.tearDown(setup_error=True)
            return
        return 
    def tearDown(self, setup_error=False) -> None:
        if not setup_error: 
            os.chdir("..")
        shutil.rmtree("temp")
        return

    def test_0(self):
        source = Source(
            label='test_source', 
            environment="local", 
            file="data.json"
        )
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        
        #self.assertListEqual(result_to_assert, result['out_column'].values.tolist())
        return

    def test_1_columns_auto(self):
        source = Source(
            label='test_source_1', 
            environment="local", 
            file="data.json",
            columns="auto"
        )
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        
        global_columns_sources_mapping = \
            self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='pass').content
        test_source_1_columns = global_columns_sources_mapping['test_source_1']
        self.assertDictEqual(self.test_assertResult_file['test_source_1'], test_source_1_columns)
        return

    def test_2_columns_specified(self):
        source = Source(
            label='test_source_2', 
            environment="local", 
            file="data.json",
            columns=[
                SampleDate(name="sample_date")
            ]
        )
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()

        global_columns_sources_mapping = \
            self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='pass').content
        test_source_2_columns = global_columns_sources_mapping['test_source_2']
        self.assertDictEqual(self.test_assertResult_file['test_source_2'], test_source_2_columns)
        return

if __name__ == '__main__':
    unittest.main()


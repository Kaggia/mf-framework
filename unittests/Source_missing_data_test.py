import os
import json
import unittest
import shutil
import pandas as pd
import io 

from mosaic_framework.environment.source import Source
from mosaic_framework.environment.geospatial_source import GeospatialSource
from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.environment.columns.columns import *

class TestMissingDataSource(unittest.TestCase):
    """
    Testing Source:
        - test_hourly_missing_1_point:Source with an input where is missing 1 point. (hourly)
        - test_daily_missing_1_point:Source with an input where is missing 1 point. (daily)
        - test_hourly_missing_2_points_nonfollowing:Source with an input where are missing 2 non following points. (hourly)
        - test_daily_missing_2_points_nonfollowing:Source with an input where are missing 2 non following points. (daily)
        - test_hourly_missing_2_points_following:Source with an input where are missing 2 following points. (hourly)
        - test_daily_missing_2_points_following:Source with an input where are missing 2 following points. (daily)
        - test_hourly_missing_n_points_following:Source with an input where are missing more than 2 following points. (hourly)
        - test_daily_missing_n_points_following:Source with an input where are missing more than 2 following points. (daily)


    """

    def setUp(self) -> None:
        try:
            #Load all files needed for the tests one time only
            self.data_folder         = "unittests/data/Source_missing_data/"
            self.temp_folder         = "temp/"
            self.temp_data_folder    = "temp/data/"

            data_filenames = [f for f in os.listdir(self.data_folder)]
            os.mkdir(self.temp_folder)
            os.mkdir(self.temp_data_folder)
            for dfn in data_filenames:
                shutil.copyfile(self.data_folder + dfn, f'{self.temp_data_folder}{dfn}')
            os.chdir(self.temp_folder)
        except:
            print("Error on setUp")
            setup_error=True
            self.tearDown(setup_error=setup_error)
        return 
    
    def tearDown(self, setup_error=False) -> None:
        if not setup_error: 
            os.chdir("..")
        shutil.rmtree(self.temp_folder)
        return

    def test_hourly_missing_1_point(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_hourly_missing_1_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_hourly_missing_1_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_daily_missing_1_point(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_daily_missing_1_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_daily_missing_1_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_hourly_missing_2_points_nonfollowing(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_hourly_missing_2_nofollowing_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_hourly_missing_2_nofollowing_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_daily_missing_2_points_nonfollowing(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_daily_missing_2_nofollowing_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_daily_missing_2_nofollowing_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_hourly_missing_2_points_following(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_hourly_missing_2_following_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_hourly_missing_2_following_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_daily_missing_2_points_following(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_daily_missing_2_following_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_daily_missing_2_following_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_hourly_missing_n_points_following(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_hourly_missing_n_following_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_hourly_missing_n_following_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

    def test_daily_missing_n_points_following(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)
        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=self.shared_memory)
        gs.set_storage(data_storage=self.data_storage)
        gs.run()

        test_assertResult_file = json.load(open('data/' + f"assertResult_daily_missing_n_following_point.json", "r+"))['data']

        source = Source(
            label='test_source', 
            environment="local", 
            file="assertResult_daily_missing_n_following_point.json"
        )
        
        source.set_storage(data_storage=self.data_storage)
        source.set_memory(shared_memory=self.shared_memory)

        source.run()
        result_to_assert = pd.DataFrame(test_assertResult_file)
        filled_df        = pd.DataFrame(source.data_storage.get_resource(label='test_source', error_policy='pass').get_data())
        self.assertDictEqual(filled_df.to_dict(), result_to_assert.to_dict())
        return

if __name__ == '__main__':
    unittest.main()

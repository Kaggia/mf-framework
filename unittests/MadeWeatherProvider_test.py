from __future__ import annotations
from typing import List, TYPE_CHECKING

import unittest
import inspect
import json
from copy import deepcopy
import pandas as pd

from mosaic_framework.vault.vault import MosaicVault
from mosaic_framework.vault.secret import APIKeySecret
from mosaic_framework.environment.geospatial_source import GeospatialSource
from mosaic_framework.data_layer.data_retriever_protocol import ProtocolDataRetriever
from mosaic_framework.data_layer.exceptions import ParameterNotAllowedException, APIPermissionException
from mosaic_framework.data_layer.api import GetApi
from mosaic_framework.validation.input_validation import InputDataValidator
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_layer.made_weather_provider import MadeWeatherProvider 

from mosaic_framework.config.data_layer_configuration import MADE_API_URL_V1


# if TYPE_CHECKING:
    
#     MosaicDataStorageType  = MosaicDataStorage
#     MosaicSharedMemoryType  = MosaicSharedMemory



# from mosaic_framework.core.value_factors import MapValuesRule

class TestMadeWeatherProvider(unittest.TestCase):
    """
    Testing MadeWeatherProvider class
        .
    """

    def setUp(self) -> None:
        self.maxDiff     = None
        self.data_folder = "unittests/data/MadeWeatherProvider/"
        return 
    def tearDown(self) -> None:
        return

    def test_0(self):
        '''test with costant mapping_engine'''
        sharedMemory = MosaicSharedMemory(DEBUG=True)
        dataStorage  = MosaicDataStorage(DEBUG=True)
        dataStorage.allocate()

        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=sharedMemory)
        gs.set_storage(data_storage=dataStorage)
        gs.run()

        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            IDV             = InputDataValidator(data=data)
            granularity     = IDV.get_granularity(dt_series=pd.to_datetime(data['date']))
            lat = sharedMemory.get_variable(key='geospatial_data').content['latitude']
            lon = sharedMemory.get_variable(key='geospatial_data').content['longitude']
            MWP = MadeWeatherProvider(coordinates={'latitude':lat, 'longitude': lon}, version=1, weather_parameters_mapping={'date' :'SampleDate', 'temperature':'Temperature'}, mapping_engine = 'constant')
            made_df = MWP.get_data(granularity=granularity, start_date='2024-09-09')
            # made_df = MWP.mapping_columns(df_made=made_df)
            partial_made_df = MWP.get_selected_data(time_start="2024-09-09 15:00", time_end= "2024-09-09 16:00", made_df=made_df)
            final_df        = MWP.merge_made_data(partial_made_df=partial_made_df, original_df=data, dt_column_name='date')
            result_to_assert= pd.DataFrame(test_data_file['assertResult'])
            columns_result   = list(final_df.columns)
            columns_result.sort()
            columns_toAssert = list(result_to_assert.columns)
            columns_toAssert.sort()
            self.assertListEqual(columns_result, columns_toAssert)
        return


    def test_1(self):
        '''test with costant mapping_engine'''
        sharedMemory = MosaicSharedMemory(DEBUG=True)
        dataStorage = MosaicDataStorage(DEBUG=True)
        dataStorage.allocate()

        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=sharedMemory)
        gs.set_storage(data_storage=dataStorage)
        gs.run()

        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            IDV             = InputDataValidator(data=data)
            granularity     = IDV.get_granularity(dt_series=pd.to_datetime(data['date']))
            lat = sharedMemory.get_variable(key='geospatial_data').content['latitude']
            lon = sharedMemory.get_variable(key='geospatial_data').content['longitude']
            MWP = MadeWeatherProvider(coordinates={'latitude':lat, 'longitude': lon}, version=1, weather_parameters_mapping={'SampleDate': 'date', 'Temperature': 'temperature'}, mapping_engine = 'dynamic')
            made_df         = MWP.get_data(granularity=granularity, start_date='2024-09-09')
            partial_made_df = MWP.get_selected_data(time_start="2024-09-09 16:00", time_end= "2024-09-09 16:00", made_df=made_df)
            final_df        = MWP.merge_made_data(partial_made_df=partial_made_df, original_df=data, dt_column_name='date')
            result_to_assert= pd.DataFrame(test_data_file['assertResult'])
            columns_result   = list(final_df.columns)
            columns_result.sort()
            columns_toAssert = list(result_to_assert.columns)
            columns_toAssert.sort()
            self.assertListEqual(columns_result, columns_toAssert)
        return
    
if __name__ == '__main__':
    unittest.main()


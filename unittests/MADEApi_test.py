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
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.data_storage.data_storage import MosaicDataStorage

from mosaic_framework.config.data_layer_configuration import MADE_API_URL_V1


# if TYPE_CHECKING:
    
#     MosaicDataStorageType  = MosaicDataStorage
#     MosaicSharedMemoryType  = MosaicSharedMemory



# from mosaic_framework.core.value_factors import MapValuesRule

class TestMADEApi(unittest.TestCase):
    """
    Testing MADEApi trhough the GetAPI class
        .
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/MADEApi/"
        self.maxDiff     = None
        return 
    def tearDown(self) -> None:
        return

    def test_0(self):
        sharedMemory = MosaicSharedMemory(DEBUG=True)
        dataStorage = MosaicDataStorage(DEBUG=True)
        dataStorage.allocate()

        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=sharedMemory)
        gs.set_storage(data_storage=dataStorage)
        gs.run()

        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            api_parameters = {
                'lat' : sharedMemory.get_variable(key='geospatial_data').content['latitude'],
                'lon' : sharedMemory.get_variable(key='geospatial_data').content['longitude'],
                'historical_1granularity' : 'historical_1d',
                'data_points_data_type_plural' : '1_months'
            }
            GA        = GetApi(stage='dev')
            made_data = GA.retrieve(api_url=MADE_API_URL_V1, api_parameters=api_parameters)
            data      = json.loads(made_data['body'])
            data.pop('statistics')
            test_data_file  = json.load(test_data_f)
            result_to_assert= sorted(test_data_file['assertResult'])
            data_list_of_params=sorted(list(data['data']['historical'].keys()))
            # print(data['data']['historical'])
            # only check the keys of the response, not possible to get static value from call
            self.assertListEqual(data_list_of_params, result_to_assert)
        return

    def test_1(self):
        sharedMemory = MosaicSharedMemory(DEBUG=True)
        dataStorage  = MosaicDataStorage(DEBUG=True)
        dataStorage.allocate()

        gs = GeospatialSource(label='test_geospacialsource', latitude=43.3445, longitude=8.2314)
        gs.set_memory(shared_memory=sharedMemory)
        gs.set_storage(data_storage=dataStorage)
        gs.run()

        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            api_parameters = {
                'lat' : sharedMemory.get_variable(key='geospatial_data').content['latitude'],
                'lon' : sharedMemory.get_variable(key='geospatial_data').content['longitude'],
                'historical_1granularity' : 'historical_1h',
                'data_points_data_type_plural' : '1_day'
            }
            GA = GetApi(stage='dev')
            made_data = GA.retrieve(api_url=MADE_API_URL_V1, api_parameters=api_parameters)
            data  = json.loads(made_data['body'])
            data.pop('statistics')
            test_data_file  = json.load(test_data_f)
            result_to_assert= sorted(test_data_file['assertResult'])
            data_list_of_params=sorted(list(data['data']['historical'].keys()))
            # print(data['data']['historical'])
            # only check the keys of the response, not possible to get static value from call
            self.assertListEqual(data_list_of_params, result_to_assert)
        return

if __name__ == '__main__':
    unittest.main()


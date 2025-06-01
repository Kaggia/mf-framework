import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.environment.columns.detect_engine import LevenshteinDistanceColumnDetectEngine
from mosaic_framework.engine.module_parser import ModuleParser
import mosaic_framework.environment.columns.columns

class TestLevenshteinDistanceColumnDetectEngine(unittest.TestCase):
    """
    Testing LevenshteinDistanceColumnDetectEngine:
        test_0                  : Testing the whole algorithm, plain no details.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/LevenshteinDistanceColumnDetectEngine/"
        self.modules     = ModuleParser().get(module=mosaic_framework.environment.columns.columns)
        return 
    def tearDown(self) -> None:
        return
     
    def test_0_base(self): 
        data_columns = None    
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            data_columns  = json.load(test_data_f)['data']
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        ldcde        = LevenshteinDistanceColumnDetectEngine(duplicate_policy='raise')
        mapping      = ldcde.run(
            data_columns=data_columns, 
            classes=self.modules)
        self.assertDictEqual(mapping, assert_results)
        return
    def test_1_duplicates(self): 
        data_columns = None    
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            data_columns  = json.load(test_data_f)['data']
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        ldcde        = LevenshteinDistanceColumnDetectEngine(duplicate_policy='best')
        mapping      = ldcde.run(
            data_columns=data_columns, 
            classes=self.modules)
        self.assertDictEqual(mapping, assert_results)
        return
    def test_2_validation_fallback_to_generic_column(self): 
        data_columns = None    
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.xlsx", "rb") as test_data_f:
            data_columns  = list(pd.read_excel(test_data_f).columns)
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        ldcde        = LevenshteinDistanceColumnDetectEngine(duplicate_policy='best')
        mapping      = ldcde.run(
            data_columns=data_columns, 
            classes=self.modules)
        self.assertDictEqual(mapping, assert_results)
        return


if __name__ == '__main__':
    unittest.main()


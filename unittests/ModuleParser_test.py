import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.environment.columns.detect_engine import LevenshteinDistanceColumnDetectEngine
from mosaic_framework.engine.module_parser import ModuleParser
from mosaic_framework.environment.columns.columns import Column
import mosaic_framework.environment.columns.columns

class TestModuleParser(unittest.TestCase):
    """
    Testing LevenshteinDistanceColumnDetectEngine:
        test_0                  : Testing the whole algorithm, plain no details.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/ModuleParser/"
        return 
    def tearDown(self) -> None:
        return
     
    def test_0_base(self): 
        module_parser     = ModuleParser()
        modules           = module_parser.get(
            module=mosaic_framework.environment.columns.columns,
            filter_by_parent=Column)
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        self.assertListEqual(sorted([c.__name__ for c in modules]), sorted(assert_results))
        return

if __name__ == '__main__':
    unittest.main()


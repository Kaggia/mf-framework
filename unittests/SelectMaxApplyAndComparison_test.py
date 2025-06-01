import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.output_factors import SelectMaxApplyAndComparison
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class SelectMaxApplyAndComparisonTest(unittest.TestCase):
    """
    Testing SelectMaxApplyAndComparison:
        test_0: Base case.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/SelectMaxApplyAndComparison/"
        self.rules_hub_config      = MODEL.get("data").get("rules_hub")
        self.rules_hub = MosaicRulesHub(config=self.rules_hub_config)
        self.rules_hub.add_variable("debug", content=True, is_immutable=True)

        return 
    
    def tearDown(self) -> None:
        return

    def test_0_base_case(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = SelectMaxApplyAndComparison(
                column='infection', 
                target=['column_1', 'column_2'], 
                condition='goet1', 
                ref=0)
            rule.set_rules_hub(self.rules_hub)
            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return

    def test_1(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = SelectMaxApplyAndComparison(
                column='test1', 
                target=['column_1', 'column_2'], 
                condition='goet1.0', 
                ref=0)
            rule.set_rules_hub(self.rules_hub)
            daily_data, _ = rule.evaluate(data=data)
            print(daily_data)
            print(_)
            self.assertListEqual(result_to_assert, daily_data['test1'].values.tolist())
        return
    
    def test_2(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = SelectMaxApplyAndComparison(
                column='test2', 
                target=['column_1', 'column_2'], 
                condition='goet1.0', 
                ref=0)
            rule.set_rules_hub(self.rules_hub)
            daily_data, _ = rule.evaluate(data=data)
            print(daily_data)
            print(_)
            self.assertListEqual(result_to_assert, daily_data['test2'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()
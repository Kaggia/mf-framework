import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.output_factors import ApplyWindowing
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class ApplyWindowingTest(unittest.TestCase):
    """
    Testing ApplyWindowing:
        - test_0_one_rule_w_400, with a window [4 0 0] select fnc max, window fnc sum
        - test_0_one_rule_w_310, with a window [3 1 0] select fnc max, window fnc sum
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/ApplyWindowing/"
        self.rules_hub_config      = MODEL.get("data").get("rules_hub")
        self.rules_hub = MosaicRulesHub(config=self.rules_hub_config)
        self.rules_hub.add_variable("debug", content=True, is_immutable=True)
        self.rules_hub.add_variable("granularity", content='daily', is_immutable=True)

        return 
    
    def tearDown(self) -> None:
        return

    def test_0_one_rule_w_400(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = ApplyWindowing(
                column='infection', 
                target='column_1', 
                window_past=4, 
                window_current=0, 
                window_future=0,
                select_fnc='max', 
                window_fnc='sum')
            rule.set_rules_hub(self.rules_hub)

            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return

    def test_0_one_rule_w_310(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = ApplyWindowing(
                column='infection', 
                target='column_1', 
                window_past=3, 
                window_current=1, 
                window_future=0,
                select_fnc='max', 
                window_fnc='sum')
            rule.set_rules_hub(self.rules_hub)

            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return

    def test_1_one_rule_w_400(self):
        self.rules_hub = MosaicRulesHub(config=self.rules_hub_config)
        self.rules_hub.add_variable("debug", content=True, is_immutable=True)
        self.rules_hub.add_variable("granularity", content='hourly', is_immutable=True)

        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = ApplyWindowing(
                column='infection', 
                target='column_1', 
                window_past=4, 
                window_current=0, 
                window_future=0,
                select_fnc='max', 
                window_fnc='sum')
            rule.set_rules_hub(self.rules_hub)

            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()
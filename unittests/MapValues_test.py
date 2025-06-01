import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.comparative_factors import SimpleComparativeRule
from mosaic_framework.core.value_factors import MapValuesRule
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class TestMapValues(unittest.TestCase):
    """
    Testing MapValues:
        test_0: tests a simple case, with no on_condition.
        test_1: tests a simple case, with on_condition.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/MapValuesRule/"
        self.rules_hub_config      = MODEL.get("data").get("rules_hub")
        self.rules_hub = MosaicRulesHub(config=self.rules_hub_config)
        self.rules_hub.add_variable("debug", content=True, is_immutable=True)

        return 
    def tearDown(self) -> None:
        return

    def test_0(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            map_values_rule = MapValuesRule(
                target='in_column',
                column='out_column',
                mapping={2902:2, 2903:3, 2904:4,'default':0},
                debug=False)
            map_values_rule.set_rules_hub(self.rules_hub)
            result = map_values_rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['out_column'].values.tolist())
        return
    def test_1(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            map_values_rule = MapValuesRule(
                target='in_column',
                column='out_column',
                mapping={2902:2, 2903:3, 2904:4,'default':0},
                on_condition=SimpleComparativeRule(
                    column="condition_column",
                    target='in_column', 
                    condition='gt2902'),
                debug=False)
            map_values_rule.set_rules_hub(self.rules_hub)
            result = map_values_rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['out_column'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()


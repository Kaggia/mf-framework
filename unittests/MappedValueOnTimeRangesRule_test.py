import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.value_factors import MappedValueOnTimeRangesRule
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class TestMappedValueOnTimeRangesRule(unittest.TestCase):
    """
    Testing MappedValueOnTimeRangesRule:
        test_0: tests a simple case, with no on_condition.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/MappedValueOnTimeRangesRule/"
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
            map_value_on_tf_rule = MappedValueOnTimeRangesRule(
                column="out_column", 
                target="sampleDate", 
                mapping={
                    "01-02 to 01-03"   : 1, 
                    "01-04 to 01-06"   : 2, 
                    "01-07 to 01-09"   : 3, 
                    "default"          : 0.0},
                debug=True)
            map_value_on_tf_rule.set_rules_hub(self.rules_hub)
            result = map_value_on_tf_rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['out_column'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()


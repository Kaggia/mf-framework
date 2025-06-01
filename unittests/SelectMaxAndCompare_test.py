import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.output_factors import SelectMaxAndCompare
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class SelectMaxAndCompareTest(unittest.TestCase):
    """
    Testing SelectMaxAndCompare:
        test_0: Base case.
        test_1: Base case with a list of target containing one only.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/SelectMaxAndCompare/"
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
            rule = SelectMaxAndCompare(
                column='infection', 
                target='column_1', 
                condition='goet2', 
                ref=0)
            rule.set_rules_hub(self.rules_hub)

            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return

    def test_1_base_case(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            rule = SelectMaxAndCompare(
                column='infection', 
                target=['column_1'], 
                condition='goet2', 
                ref=0)
            rule.set_rules_hub(self.rules_hub)
            daily_data, _ = rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, daily_data['infection'].values.tolist())
        return


if __name__ == '__main__':
    unittest.main()
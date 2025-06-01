import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.datetime_factors import getHourRule
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class TestgetHourRule(unittest.TestCase):
    """
    Testing getHourRule:
        test_0: tests a simple case, with no on_condition.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/getHourRule/"
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
            gHR = getHourRule(target='sampleDate', column='hours')
            gHR.set_rules_hub(self.rules_hub)
            result = gHR.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['hours'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()
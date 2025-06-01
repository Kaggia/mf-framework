import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.datetime_factors import isNightTimeRule
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class TestisNightTimeRule(unittest.TestCase):
    """
    Testing isNightTimeRule:
        test_0: tests a simple case, with no on_condition.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/isNightTimeRule/"
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
            iNTR = isNightTimeRule(target='sampleDate', column='is_night_time', range=[8,20])
            iNTR.set_rules_hub(self.rules_hub)
            print(iNTR.rules_hub)
            result = iNTR.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['is_night_time'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()
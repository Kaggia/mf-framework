import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.datetime_factors import isDayTimeRule
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL

class TestisDayTimeRule(unittest.TestCase):
    """
    Testing isDayTimeRule:
        test_0: tests a simple case, with no on_condition.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/isDayTimeRule/"
        self.rules_hub_config      = MODEL.get("data").get("rules_hub")
        self.rules_hub = MosaicRulesHub(config=self.rules_hub_config)
        self.rules_hub.add_variable("debug", content=True, is_immutable=True)

        return 
    def tearDown(self) -> None:
        return

    def test_0(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file   = json.load(test_data_f)
            data             = pd.DataFrame(data=test_data_file['data'])
            result_to_assert = test_data_file['assertResult']
            is_day_time_rule = isDayTimeRule(target='sampleDate', column='is_day_time', range=[8,20])
            is_day_time_rule.set_rules_hub(rules_hub=self.rules_hub)
            print(is_day_time_rule.rules_hub)
            result           = is_day_time_rule.evaluate(data=data)
            self.assertListEqual(result_to_assert, result['is_day_time'].values.tolist())
        return

if __name__ == '__main__':
    unittest.main()
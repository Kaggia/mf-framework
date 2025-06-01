import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.output_model import OutputModel
from mosaic_framework.core.output_factors import SelectMaxAndCompare
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.config.configuration import MODEL
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule

class TestOutputModel(unittest.TestCase):
    """
    TestOutputModel:
        test_0  : ...
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/OutputModel/"
        self.config      = MODEL.get("data").get("rules_hub")
        self.rules_hub   = MosaicRulesHub(config=self.config)
        self.rules_hub.add_variable("debug", content=False, is_immutable=True)

        return 
    def tearDown(self) -> None:
        return
     
    def test_0_one_rule(self): 
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            data          = json.load(test_data_f)['data']

        output_model = OutputModel(
            label='test_output_model',
            previsionDay=pd.to_datetime('2024-01-01T00:00:00+00:00'),
            days=5,
            data=pd.DataFrame(data=data),
            history=(1,0,0),
            rules_hub=self.rules_hub
        )

        r1 = SimpleComparativeRule(column='avgtemp_cond', target='avgTemp', condition='goet18.0')
        r1.set_rules_hub(rules_hub=self.rules_hub)
        output_model.add_factor(factor=r1)

        rf = SelectMaxAndCompare(column='infection', target='avgtemp_cond', condition='goet1.0', ref=0)
        rf.set_rules_hub(rules_hub=self.rules_hub)
        output_model.set_output_rule(factor=rf)
        results, compact_results = output_model.estimate()

        self.assertListEqual(assert_results, compact_results['infection'].tolist())
        
        return

    def test_1_multiple_rules(self): 
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            assert_results= json.load(test_data_f)['assertResult']
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            data          = json.load(test_data_f)['data']

        output_model = OutputModel(
            label='test_output_model',
            previsionDay=pd.to_datetime('2024-01-01T00:00:00+00:00'),
            days=5,
            data=pd.DataFrame(data=data),
            history=(1,0,0),
            rules_hub=self.rules_hub
        )

        r1 = SimpleComparativeRule(column='avgtemp_cond', target='avgTemp', condition='goet18.0')
        r1.set_rules_hub(rules_hub=self.rules_hub)
        output_model.add_factor(factor=r1)

        rf   = SelectMaxAndCompare(column='infection_0', target='avgtemp_cond', condition='goet1.0', ref=0)
        rf_2 = SelectMaxAndCompare(column='infection_1', target='infection_0',  condition='goet1.0', ref=0)

        rf.set_rules_hub(rules_hub=self.rules_hub)
        rf_2.set_rules_hub(rules_hub=self.rules_hub)

        output_model.set_output_rule(factor=[rf, rf_2])
        results, compact_results = output_model.estimate()

        self.assertListEqual(assert_results, compact_results['infection_1'].tolist())
        

        return

if __name__ == '__main__':
    unittest.main()


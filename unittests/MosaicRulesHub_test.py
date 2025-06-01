import os
import json
import inspect
import pandas as pd
import unittest
import pandas.testing as pdt

from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.core.environment.exceptions import ImmutableRulesEnvironmentVariableUpdateException
from mosaic_framework.core.comparative_factors import SimpleComparativeRule
from mosaic_framework.config.configuration import MODEL

class TestMosaicRulesHub(unittest.TestCase):
    """
    Testing MosaicRulesHub:
        - test_add_variable: Adding a single variable.
        - test_get_variable: Get the value of a variable.
        - test_update_variable_mutable: Update a mutable variable.
        - test_update_variable_immutable: Update a immutable variable.
        - test_remove_implicit_columns: Remove implicit columns.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/MosaicRulesHub/"
        self.config      = MODEL.get("data").get("rules_hub")
        return 
    def tearDown(self) -> None:
        return
        
    def test_add_variable(self):
        rules_hub = MosaicRulesHub(config=self.config)
        rules_hub.add_variable("test_var", content="TEST", is_immutable=True)

        self.assertEqual(rules_hub.get_variable('test_var').content, "TEST")
        return

    def test_get_variable(self):
        rules_hub = MosaicRulesHub(config=self.config)
        rules_hub.add_variable("test_var", content="TEST", is_immutable=True)

        self.assertEqual(rules_hub.get_variable('test_var').content, "TEST")
        return

    def test_update_variable_mutable(self):
        rules_hub = MosaicRulesHub(config=self.config)
        rules_hub.add_variable("test_var", content="TEST", is_immutable=False)
        rules_hub.update_variable(key='test_var', new_content='NEW_TEST')
        self.assertEqual(rules_hub.get_variable('test_var').content, "NEW_TEST")
        return
    
    def test_update_variable_immutable(self):
        rules_hub = MosaicRulesHub(config=self.config)
        rules_hub.add_variable("test_var", content="TEST", is_immutable=True)
        with self.assertRaises(ImmutableRulesEnvironmentVariableUpdateException):
            rules_hub.update_variable(key='test_var', new_content='NEW_TEST')

        return

    def test_remove_implicit_columns(self):
        rules_hub = MosaicRulesHub(config=self.config)

        r1 = SimpleComparativeRule(column='r1', target='weather_col', condition='gt0')
        r2 = SimpleComparativeRule(target='weather_col', condition='gt0', is_implicit=True)

        rules_hub.register(r1)
        rules_hub.register(r2)

        df = pd.DataFrame(data=[
            {'weather_col':1.0, 'r1':2.0, r2.column:3.0},
            {'weather_col':2.0, 'r1':3.0, r2.column:4.0},
            {'weather_col':3.0, 'r1':4.0, r2.column:5.0}])
        
        assert_df = pd.DataFrame(data=[
            {'weather_col':1.0, 'r1':2.0},
            {'weather_col':2.0, 'r1':3.0},
            {'weather_col':3.0, 'r1':4.0}])
        
        df = rules_hub.remove_implicit_columns(data=df)

        pdt.assert_frame_equal(df, assert_df)

        return


if __name__ == '__main__':
    unittest.main()


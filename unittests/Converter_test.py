import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.data_storage.converters import Converter

class TestConverter(unittest.TestCase):
    """
    Testing InputDataValidator:
        test_get_method_name                  : get_method_name test method case.
        test_convert_from_dataframe_to_records: convert_from_dataframe_to_records test method case.
        test_to_resource_format               : to_resource_format test method case.
        test_to_data_format                   : to_data_format test method case.
        test_0                                : dataframe_to_records test case.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/Converter/"
        return 
    def tearDown(self) -> None:
        return
    
    def test_get_method_name(self):
        data          = pd.DataFrame(data=[{'sample_date': "2020-01-01 00:00"}])
        input_format  = "dataframe"
        output_format = "json"
        method_name = Converter().get_method_name(
            data=data,
            mapping_out_key="extensions",
            output_type="json")
        self.assertEqual(method_name, f"convert_from_{input_format}_to_{output_format}")
        return

    def test_convert_from_dataframe_to_records(self):
        data          = pd.DataFrame(data=[{'sample_date': "2020-01-01 00:00"}])
        records = Converter().convert_from_dataframe_to_records(data=data)
        self.assertListEqual(records, [{'sample_date': "2020-01-01 00:00"}])
        return

    def test_to_resource_format(self):
        data            = pd.DataFrame(data=[{'sample_date': "2020-01-01 00:00"}])
        result_to_assert= [{'sample_date': "2020-01-01 00:00"}]
        converted_data  = Converter().to_resource_format(data=data, file_format="json")
        self.assertListEqual(json.loads(converted_data), result_to_assert)
        return
    
    def test_to_data_format(self):
        data            = pd.DataFrame(data=[{'sample_date': "2020-01-01 00:00"}])
        result_to_assert= pd.DataFrame(data=[{'sample_date': "2020-01-01 00:00"}])
        converted_data  = Converter().to_data_format(data=data, data_format="dataframe")
        self.assertTrue(converted_data.equals(result_to_assert))
        return
    
    def test_0(self):
        """dataframe_to_records test case.
        """
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= test_data_file['assertResult']
            converted_data  = Converter().to_resource_format(data=data, file_format="json")
            self.assertListEqual(result_to_assert, json.loads(converted_data))
        return

if __name__ == '__main__':
    unittest.main()


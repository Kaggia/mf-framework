import unittest
from datetime import datetime, timezone
import pandas as pd

from mosaic_framework.dt.datetime_parser import DatetimeParser
from mosaic_framework.dt.exceptions import DateParsingException

class TestDatetimeParser(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.parser = DatetimeParser()

    # Test parse_single with valid ISO8601 string
    def test_parse_single_valid_iso8601(self):
        date_str = "2024-01-01T12:00:00+02:00"
        expected = "2024-01-01T12:00:00.000000+0200"
        result = self.parser.parse_single(date_str)
        self.assertEqual(result, expected)

    # Test parse_single with valid date string without timezone
    def test_parse_single_valid_no_timezone(self):
        date_str = "2024-01-01T12:00:00"
        expected = "2024-01-01T12:00:00.000000+0000"
        result = self.parser.parse_single(date_str)
        self.assertEqual(result, expected)

    # Test parse_single with different valid date formats
    def test_parse_single_different_formats(self):
        dates = {
            "2024-01-01T12:00:00Z": "2024-01-01T12:00:00.000000+0000",
            "2024-01-01 12:00:00+02:00": "2024-01-01T12:00:00.000000+0200",
            "January 1, 2024 12:00 PM": "2024-01-01T12:00:00.000000+0000",
        }
        for input_date, expected in dates.items():
            with self.subTest(input_date=input_date):
                result = self.parser.parse_single(input_date)
                self.assertEqual(result, expected)

    # Test parse_batch with list of valid dates
    def test_parse_batch_list_valid(self):
        date_list = [
            "2024-01-01T12:00:00+02:00",
            "2024-01-02T15:30:00Z",
            "January 3, 2024 09:15 AM"
        ]
        expected = [
            "2024-01-01T12:00:00.000000+0200",
            "2024-01-02T15:30:00.000000+0000",
            "2024-01-03T09:15:00.000000+0000"
        ]
        result = self.parser.parse_batch(date_list)
        self.assertEqual(result, expected)

    # Test parse_batch with mixed types in list
    def test_parse_batch_list_mixed_types(self):
        date_list = [
            "2024-01-01T12:00:00+02:00",
            12345,  # Non-string type
            "January 3, 2024 09:15 AM"
        ]
        with self.assertRaises(DateParsingException):
            result = self.parser.parse_batch(date_list)

    # Test parse_batch with empty list
    def test_parse_batch_list_empty(self):
        date_list = []
        expected = []
        result = DatetimeParser.parse_batch(date_list)
        self.assertEqual(result, expected)

    # Test parse_batch with dict of valid dates
    def test_parse_batch_dict_valid(self):
        date_dict = {
            "start": "2024-01-01T12:00:00+02:00",
            "end": "2024-01-02T15:30:00Z",
            "created": "January 3, 2024 09:15 AM"
        }
        expected = {
            "start": "2024-01-01T12:00:00.000000+0200",
            "end": "2024-01-02T15:30:00.000000+0000",
            "created": "2024-01-03T09:15:00.000000+0000"
        }
        result = DatetimeParser.parse_batch(date_dict)
        self.assertEqual(result, expected)

    # Test parse_batch with empty dict
    def test_parse_batch_dict_empty(self):
        date_dict = {}
        expected = {}
        result = DatetimeParser.parse_batch(date_dict)
        self.assertEqual(result, expected)

    # Test parse_batch with pandas DataFrame of valid dates
    def test_parse_batch_dataframe_valid(self):
        df            = pd.DataFrame([{"sampledate": "2024-01-01T12:00:00+02:00",      "temperature": 20.0}])
        expected_df   = pd.DataFrame([{"sampledate": "2024-01-01T12:00:00.000000+0200","temperature": 20.0}])

        result_df     = self.parser.parse_batch(b=df)
        print(df.head(5))
        print(expected_df.head(5))
        print(result_df.head(5))

        pd.testing.assert_frame_equal(result_df, expected_df)

    # Test parse_batch with empty DataFrame
    def test_parse_batch_empty_dataframe(self):
        df = pd.DataFrame()
        expected_df = pd.DataFrame()
        result_df = DatetimeParser.parse_batch(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    # Test parse_batch with unsupported type
    def test_parse_batch_unsupported_type(self):
        unsupported_input = "2024-01-01T12:00:00+02:00"
        with self.assertRaises(DateParsingException):
            DatetimeParser.parse_batch(unsupported_input)

    # Test parse_batch with nested structures (should raise exception)
    def test_parse_batch_nested_structure(self):
        nested_input = {
            "start": "2024-01-01T12:00:00+02:00",
            "details": {
                "end": "2024-01-02T15:30:00Z"
            }
        }
        with self.assertRaises(DateParsingException):
            DatetimeParser.parse_batch(nested_input)

    # Test parse_single with different output formats
    def test_parse_single_different_output_format(self):
        parser = DatetimeParser(output_format="%d/%m/%Y %H:%M:%S %z")
        date_str = "2024-01-01T12:00:00+02:00"
        expected = "01/01/2024 12:00:00 +0200"
        result = parser.parse_single(date_str)
        self.assertEqual(result, expected)

    # Test parse_single with microseconds
    def test_parse_single_with_microseconds(self):
        date_str = "2024-01-01T12:00:00.123456+02:00"
        expected = "2024-01-01T12:00:00.123456+0200"
        result = self.parser.parse_single(date_str)
        self.assertEqual(result, expected)

    # Test parse_single with different timezone representations
    def test_parse_single_different_timezone_representations(self):
        dates = {
            "2024-01-01T12:00:00+0200": "2024-01-01T12:00:00.000000+0200",
            "2024-01-01T12:00:00+02:00": "2024-01-01T12:00:00.000000+0200",
            "2024-01-01T12:00:00Z": "2024-01-01T12:00:00.000000+0000"
        }
        for input_date, expected in dates.items():
            with self.subTest(input_date=input_date):
                result = self.parser.parse_single(input_date)
                self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
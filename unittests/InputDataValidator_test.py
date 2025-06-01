import os
import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.validation.input_validation import InputDataValidator
from mosaic_framework.validation.input_data_filler import InputDataFiller
from mosaic_framework.validation.filling_policies import AverageFillingPolicy, WeatherProviderFillingPolicy


class TestInputDataValidator(unittest.TestCase):
    """
    Testing InputDataValidator:
        test_0: tests AverageFillingPolicy.
    """
    @staticmethod  
    def find_data_column(data):
        '''
        Method to get the column name containing the dates
        '''
        df = data.head(20)
        mask = df.astype(str).apply(lambda x : x.str.match(r'\d{2,4}-*\/*\d{2}-*\/*\d{2,4}.*').all())
        dt_column = mask[mask==True]
        # check on the result found
        if len(dt_column) != 1:
            raise ValueError('No column or more than one column has been recognized as datetime. Check your input data.')
        
        return dt_column.index[0]

    def setUp(self) -> None:
        self.data_folder = "unittests/data/InputDataValidator/"
        return 
    
    def tearDown(self) -> None:
        return

    def test_find_data_column(self):
        df  = pd.DataFrame(data={'dt_column': ['12-09-1998'], 'a': [0]})
        IDV = InputDataValidator(data=df)
        dt_column = IDV.find_data_column(data=df)
        self.assertEqual(dt_column, f"dt_column")
        return
    
    def test_get_granularity(self):
        df              = pd.DataFrame(data={'dt_column': ['12-09-1998 00:00', '12-09-1998 01:00', '12-09-1998 02:00'], 'a': [0,2,3]})
        df['dt_column'] = pd.to_datetime(df['dt_column'])
        IDV             = InputDataValidator(data=df)
        granularity     = IDV.get_granularity(dt_series=df['dt_column'])
        self.assertEqual(granularity, f"H")
        return

    def test_list_of_missing_record(self):
        df = pd.DataFrame(data={'dt_column': ['12-09-1998 00:00', '12-09-1998 02:00', '12-09-1998 03:00', '12-09-1998 06:00'], 'a': [0,2,3,4]})
        df['dt_column'] = pd.to_datetime(df['dt_column'])
        IDV             = InputDataValidator(data=df)
        granularity     = IDV.get_granularity(dt_series=df['dt_column'])
        start_date      = df['dt_column'].min()
        end_date        = df['dt_column'].max()
        missing_record  = IDV.get_missing_records(dt_series=df['dt_column'],start_date=start_date, 
                                                    end_date=end_date, granularity=granularity)
        self.assertListEqual(missing_record, [pd.Timestamp('12-09-1998 01:00'), pd.Timestamp('12-09-1998 04:00'), pd.Timestamp('12-09-1998 05:00')])
        return

    def test_build_response(self):
        df = pd.DataFrame(data={'dt_column': ['12-09-1998 00:00', '12-09-1998 02:00', '12-09-1998 03:00', '12-09-1998 06:00'], 'a': [0,2,3,4]})
        df['dt_column'] = pd.to_datetime(df['dt_column'])
        IDV = InputDataValidator(data=df)
        granularity = IDV.get_granularity(dt_series=df['dt_column'])
        start_date = df['dt_column'].min()
        end_date = df['dt_column'].max()
        missing_record = IDV.get_missing_records(dt_series=df['dt_column'],start_date=start_date, 
                                                    end_date=end_date, granularity=granularity)
        response = IDV.build_response(list_missing_data_points=missing_record,
                                      granularity=granularity)
        print(response)
        manual_response = {"missing_data_points": [{pd.Timestamp('12-09-1998 01:00'):AverageFillingPolicy}, 
                                                   {pd.Timestamp('12-09-1998 04:00'): WeatherProviderFillingPolicy},
                                                   {pd.Timestamp('12-09-1998 05:00'): WeatherProviderFillingPolicy}]}
        print(manual_response)

        self.assertDictEqual(response, manual_response)
        return

    def test_0(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= pd.DataFrame(test_data_file['assertResult'])

            input_val    = InputDataValidator(data=data)
            missing_data = input_val.run()
            weather_parameters_mapping={'sampleDate':'SampleDate', 'humidity': 'Humidity', 'temperature': 'Temperature'}
            filler       = InputDataFiller(weather_parameters_mapping=weather_parameters_mapping, version=1)
            result       = filler.fill(data=data, missing_data=missing_data)

            #Assert equality with expected result
            self.assertListEqual(list(data.columns),list(result_to_assert.columns))
            IDV    = InputDataValidator(data=data)
            dt_col = IDV.find_data_column(data=data)
            col_to_validate = list(data.columns)
            col_to_validate.remove(dt_col)
            for column in col_to_validate:
                # print(column)
                # print(result_to_assert[column].tolist(), result[column].tolist())
                self.assertListEqual(result_to_assert[column].tolist(), result[column].tolist())
        return

    def test_1(self):
        with open(self.data_folder + f"{str(inspect.currentframe().f_code.co_name)}.json", "r+") as test_data_f:
            test_data_file  = json.load(test_data_f)
            data            = pd.DataFrame(data=test_data_file['data'])
            result_to_assert= pd.DataFrame(test_data_file['assertResult'])

            input_val = InputDataValidator(data=data)
            missing_data = input_val.run()
            weather_parameters_mapping={'sampleDate':'SampleDate', 'humidity': 'Humidity', 'temperature': 'Temperature'}
            filler       = InputDataFiller(weather_parameters_mapping=weather_parameters_mapping, version=1)
            result = filler.fill(data=data, missing_data=missing_data)

            #Assert equality with expected result
            self.assertListEqual(list(data.columns),list(result_to_assert.columns))
            dt_col = self.find_data_column(data)
            col_to_validate = list(data.columns)
            col_to_validate.remove(dt_col)
            print(col_to_validate)
            for column in col_to_validate:
                # print(column)
                # print(result_to_assert[column].tolist(), result[column].tolist())
                self.assertListEqual(result_to_assert[column].tolist(), result[column].tolist())
    
        return
if __name__ == '__main__':
    unittest.main()


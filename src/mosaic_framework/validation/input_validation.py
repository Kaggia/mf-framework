################################################################################
# Module:      input_validation.py
# Description: Module containing entry point for input validation pipeline.
# Author:      Stefano Zimmitti, Lorenzo Gini
# Date:        21/08/2024
# Company:     xFarm Technologies
################################################################################
import pandas as pd
from datetime import timedelta
from typing import List

from mosaic_framework.validation.exceptions import InputValidationException
from mosaic_framework.validation.filling_policies import AverageFillingPolicy, WeatherProviderFillingPolicy

class InputDataValidator():
    """
    InputDataValidator class is used to validate data, getting missing data points.

    Args:
        data (pd.DataFrame): DataFrame containing the data to be validated.
    """
    def __init__(self, data:pd.DataFrame) -> None:
        self.data = data

    @staticmethod  
    def find_data_column(data:pd.DataFrame):
        '''
        Method to get the column name containing the dates, each column is
        checked against a regular expression.
        '''

        df   = data.head(20)
        mask = df.astype(str).apply(lambda x : x.str.match(r'\d{2,4}-*\/*\d{2}-*\/*\d{2,4}.*').all())
        dt_column = mask[mask==True]
        # check on the result found
        if len(dt_column) != 1:
            raise InputValidationException('No column has been found to be a datetime.')
        
        return dt_column.index[0]
    
    @staticmethod
    def get_granularity(dt_series:pd.Series)->str:
        '''
        Static method to get granularity of the data. WARNING: the first 3 samples must be correct, no missing records!
        '''
        granularity = None
        if dt_series[0] + timedelta(days=1) < dt_series[2]:
            granularity = 'D'
        elif dt_series[0] + timedelta(hours=1) < dt_series[2]:
            granularity = 'H'
        else:
            raise InputValidationException('Granularity of your input data has not been recognised. Available are (D)aily and (H)ourly.')
        return granularity
    
    @staticmethod
    def get_missing_records(dt_series:pd.Series, start_date:pd.Timestamp, end_date:pd.Timestamp, granularity:str)->List:    
        '''
        Static method to confront the date in the inpnut and a complete set of dates.
        '''
        try:
            complete_range = pd.date_range(start_date, end_date, freq=granularity)
            complete_range = set(complete_range)
        except:
            raise InputValidationException('Granularity of your input data has not been recognised. Available are (D)aily and (H)ourly.')
        missing_dates = complete_range.difference(set(dt_series))
        return list(sorted(missing_dates))

    @staticmethod
    def build_response(list_missing_data_points:list, granularity:str)->dict:
        '''
        Static method to pairs missing value and FillingDataPolicies
        '''

        if len(list_missing_data_points) == 0:
            return {"missing_data_points":[]}
        # choosing the delta respectevely to the granularity
        delta = timedelta(days=1) if granularity == 'D' else timedelta(hours=1)
        #case with only one missing point --> AverageFillingPolicy
        if len(list_missing_data_points) == 1:
            return {"missing_data_points":[{list_missing_data_points[0]:AverageFillingPolicy}]}
        else:  
            # initialized first list value
            if list_missing_data_points[0] + delta < list_missing_data_points[1]:
                list_policies = [AverageFillingPolicy]
            else:
                list_policies = [WeatherProviderFillingPolicy]
            # loop for compleating the rest of the list
            for i in range(1,len(list_missing_data_points)-1):
                if list_missing_data_points[i] + delta < list_missing_data_points[i+1]:
                    list_policies.append(AverageFillingPolicy)
                else: 
                    list_policies.append(WeatherProviderFillingPolicy)
            # last value
            if list_missing_data_points[-2] + delta < list_missing_data_points[-1]:
                list_policies.append(AverageFillingPolicy)
            else:
                list_policies.append(WeatherProviderFillingPolicy)

            response_list = [{x:y} for (x,y) in zip(list_missing_data_points, list_policies)]
            return {"missing_data_points": response_list}

    def run(self)->dict:
        '''
        Method to validate the input data, return a list of dictionary, eventually with missing records as key and the policy to apply as value.
        '''
        
        dt_column            = self.find_data_column(self.data)
        # convert data to datetime
        self.data[dt_column] = pd.to_datetime(self.data[dt_column])
        # get granularity
        granularity = self.get_granularity(self.data[dt_column])
        start_date  = self.data[dt_column].min()
        end_date    = self.data[dt_column].max()
        # build response
        list_missing_data_points = self.get_missing_records(
            dt_series  =self.data[dt_column], 
            start_date =start_date, 
            end_date   =end_date, 
            granularity=granularity)
        res            =  self.build_response(list_missing_data_points=list_missing_data_points, granularity=granularity)
        res['details'] =  {'granularity': granularity,  'dt_column': dt_column}
        #Example of output:
        #{}
        return res
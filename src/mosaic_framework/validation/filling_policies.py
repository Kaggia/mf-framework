################################################################################
# Module:      filling_policy.py
# Description: Collection of filling policies, to be applied to dataset.
# Author:      Stefano Zimmitti, Lorenzo Gini
# Date:        21/08/2024
# Company:     xFarm Technologies
################################################################################
import pandas as pd
from datetime import timedelta
from typing import Any

from mosaic_framework.engine.exceptions import ClassNotFoundException
from mosaic_framework.validation.exceptions import DataFillingPolicyException
from mosaic_framework.data_layer.made_weather_provider import MadeWeatherProvider 

class DataFillingPolicy():
    def __init__(self, day:pd.Timestamp, weather_parameters_mapping:dict, version:int, **kwargs) -> None:
        self.day = day
        self.missing_point_threshold_lower = 0
        self.missing_point_threshold_upper = 1
        self.weather_parameters_mapping = weather_parameters_mapping
        self.version = version
    
    
    def apply(self, data:pd.DataFrame, granularity:str, dt_column:str):
        return None

class AverageFillingPolicy(DataFillingPolicy):
    '''
    Entry point to fill policy with a single missing points, use average filling policy.
    '''
    def __init__(self, **kwargs) -> None:
        super().__init__(day=kwargs.get('day', None), 
                         version=kwargs.get('version', 1), 
                         weather_parameters_mapping= kwargs.get('weather_parameters_mapping', None))
        
    def apply(self, data:pd.DataFrame, granularity:str, dt_column:str):
        '''
        Method to fill the data with the average value of the precedent and subesquent day (for all columns)
        '''
        dt_column_name = dt_column
        d = {dt_column_name: self.day}
        delta = timedelta(days=1) if granularity == 'D' else timedelta(hours=1)
        day_before = self.day - delta
        day_after = self.day + delta
        cols = list(data.columns)
        cols.remove(dt_column_name)
        for column in cols:
            value = (data[data[dt_column_name]==day_before][column].iloc[0] + data[data[dt_column_name]==day_after][column].iloc[0])/2
            d[column] = value
        data = pd.concat([data, pd.DataFrame([d])], ignore_index=True)
        data.sort_values(by=dt_column_name, inplace=True)
        return data

class WeatherProviderFillingPolicy(DataFillingPolicy):
    def __init__(self, **kwargs) -> None:
        super().__init__(day=kwargs.get('day', None), 
                         version=kwargs.get('version', 1), 
                         weather_parameters_mapping= kwargs.get('weather_parameters_mapping', None))
        self.missing_point_threshold_lower = 1
        self.missing_point_threshold_upper = 9999
        self.coordinates = kwargs.get('coordinates', None)
        self.check_coordinates()

    def check_coordinates(self):
        if not self.coordinates:
            raise DataFillingPolicyException('Coordinates are not available, maybe a Geospatial componets is not decleared')
        
    def apply(self, data:pd.DataFrame, granularity:str, dt_column:str):
        MWP =  MadeWeatherProvider(coordinates=self.coordinates, version=self.version,
                                    weather_parameters_mapping=self.weather_parameters_mapping)
        made_df = MWP.get_data(granularity=granularity, start_date=str(self.day - timedelta(days=1)))
        partial_made_df = MWP.get_selected_data(time_start=str(self.day), time_end=str(self.day), made_df=made_df)
        final_df = MWP.merge_made_data(partial_made_df=partial_made_df, original_df=data, dt_column_name=dt_column)
        return final_df
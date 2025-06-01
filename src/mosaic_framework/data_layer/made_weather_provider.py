from __future__ import annotations
from typing import List, TYPE_CHECKING

import inspect
import json
from copy import deepcopy
import pandas as pd
from datetime import datetime
from dateutil import parser

from mosaic_framework.data_layer.api import GetApi
from mosaic_framework.data_layer.exceptions import ParameterNotAllowedException, MappingEngineException
import mosaic_framework.data_layer.mapping_engine
import mosaic_framework.config.data_layer_configuration

class MadeWeatherProvider(GetApi):
    '''Class that fill with the Made api the missing data'''
    def __init__(self, coordinates:dict, version:int, weather_parameters_mapping:dict, mapping_engine = 'constant', stage='test', **kwargs) -> None:
        super().__init__(**kwargs)
        self.coordinates = coordinates
        self.weather_parameters_mapping = weather_parameters_mapping
        self.mapping_engine = mapping_engine
        self.stage   = stage
        self.version = version
        self.api_url = self.get_api_url()

    def get_api_url(self):
        '''Method to retrive the api url from the configuration file'''
        for name, data in inspect.getmembers(mosaic_framework.config.data_layer_configuration):
            if name == 'MADE_API_URL_V'+str(self.version):
                return data
        if data == None:
            raise ParameterNotAllowedException('Made version not found. Please choose another one.')

    def change_columns_name(self, made_df:pd.DataFrame)-> pd.DataFrame:
        '''Method to map the default made columns in some more starndard for the framework'''
        mapping_columns= {
                        'time':'SampleDate',
                        'precipitation': 'Rain',
                        'relativehumidity': 'Humidity', 
                        'temperature': 'Temperature', 
                        'temperature_max': 'MaximumTemperature', 
                        'temperature_min': 'MinimumTemperature',
                        'leafwetnessminutes': 'LeafWetness'
                        }
        return made_df.rename(columns=mapping_columns)

    def get_data(self, granularity:str, start_date:str) -> pd.DataFrame:
        '''
        Wrapper of the retrive function of the super class (GetApi). 
        Given the granularity and the start date get the made data and transform those to a pandas dataframe.
        '''
        n_days_in_the_past = (datetime.now() - parser.parse(start_date)).days + 1 #round to the next days
        api_parameters = {
            'lat': self.coordinates['latitude'],
            'lon' : self.coordinates['longitude'],
            'historical_1granularity': 'historical_1'+granularity.lower(),
            'data_points_data_type_plural' : str(n_days_in_the_past)+'_days'}
        raw_data = self.retrieve(api_url=self.api_url, api_parameters=api_parameters)
        data     = json.loads(raw_data['body'])
        # data.pop('statistics')
        df = pd.DataFrame(data['data']['historical'])
        df = self.change_columns_name(made_df=df)
        return df

    def mapping_columns(self, df_made)-> pd.DataFrame:
        '''Method that pass the engine specify in the initialization of the class, and maps the column accordingly with the method choosen'''
        output_df      = None
        mapping_engine = self.mapping_engine.capitalize()+'MappingEngine'
        for name, cls in inspect.getmembers(mosaic_framework.data_layer.mapping_engine):
            if name == mapping_engine:
                mapping_engine = cls(self.weather_parameters_mapping)
                output_df      = mapping_engine.map_columns(df_made=df_made)
        
        if not isinstance(output_df, pd.DataFrame):
            raise MappingEngineException('Please choose another mapping engine, the one choosen is not found.')

        if output_df.empty:
            raise MappingEngineException('The mapping engine choosen return an empty dataframe.')        
        
        return output_df

    @staticmethod
    def get_selected_data(time_start:str, time_end:str, made_df:pd.DataFrame)-> pd.DataFrame:
        '''Extract from the data retrived from the made the needed date'''
        made_df['SampleDate'] = pd.to_datetime(made_df['SampleDate'])
        return made_df[(made_df['SampleDate']>=time_start) & (made_df['SampleDate'] <= time_end)]

    
    def merge_made_data(self,partial_made_df:pd.DataFrame, original_df:pd.DataFrame, dt_column_name:str) -> pd.DataFrame:
        '''Merge the original data with the one extrtacted form the provider, clean the dataframe'''
        partial_made_df = self.mapping_columns(df_made=partial_made_df)
        col_to_keep = set(original_df.columns)#^set(partial_made_df)
        final_df = pd.concat([partial_made_df, original_df])
        # col_to_drop = set(final_df.columns).di
        final_df = final_df[list(col_to_keep)]
        final_df[dt_column_name] = pd.to_datetime(final_df[dt_column_name], utc=True)#.dt.tz_convert('Europe/London') #may a bug 
        final_df.sort_values(by=[dt_column_name], inplace=True)   
        final_df[dt_column_name] = final_df[dt_column_name].apply(lambda x: datetime.strftime(x, format='%Y-%m-%d %H:%M:%S')) 
        return final_df.reset_index(drop=True)


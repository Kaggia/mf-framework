import pandas as pd
from copy import deepcopy
from typing import Protocol

from mosaic_framework.environment.columns.detect_engine import LevenshteinDistanceColumnDetectEngine
import mosaic_framework.environment.columns.columns
from mosaic_framework.engine.module_parser import ModuleParser
from mosaic_framework.environment.exceptions import ColumnsParamNotValidException

class ProtocolColumnMapping(Protocol):    
    def run(self):
        ...

class DynamicMappingEngine(ProtocolColumnMapping):
    '''Class that map dynamicly the column of a dataframe to the one passed in the mapping.
        It is responsable to get the columns name of a pandas dataFrame and matched with the standard used in the framework. If this step does't work, it will be mapped to a generic column.
        Finallyn return the df with the changed columns name.'''
    def __init__(self, weather_parameters_mapping) -> None:
        self.weather_parameters_mapping = weather_parameters_mapping

    def get_columns(self, engine, detect_type:str, **kwargs)->dict:
        '''Get the automatic (or specified) mapping for the columns of the dataframe to the strandard's of the framework'''
        columns     = kwargs.get('columns', None)
        if detect_type == 'auto':
            module_parser = ModuleParser()
            classes       = module_parser.get(module=mosaic_framework.environment.columns.columns)
            mapping       = engine.run(classes=classes, data_columns=columns)
        elif detect_type == 'specified':
            mapping       = {c.name:c.__class__.__name__ for c in columns}
        else:
            raise ColumnsParamNotValidException("detect_type not implemented or wrong.")
        return mapping

    def map_columns(self, df_made:pd.DataFrame):
        '''Map the made column on the column associated with the dynamic name got through levenstain distance algorithm.'''
        df = deepcopy(df_made)
        detect_engine   = LevenshteinDistanceColumnDetectEngine(duplicate_policy='best')
        dynamic_mapping = self.get_columns(engine=detect_engine, columns=df.columns, detect_type='auto') #<BUG>
        mapping_made    = {key: dynamic_mapping[key] for key in df.columns if key in list(dynamic_mapping.keys())}
        df.rename(columns=mapping_made, inplace=True)
        df.rename(columns=self.weather_parameters_mapping, inplace=True)
        return df


class ConstantMappingEngine(ProtocolColumnMapping):
    def __init__(self, weather_parameters_mapping) -> None:
        self.weather_parameters_mapping = weather_parameters_mapping

    def map_columns(self, df_made:pd.DataFrame):
        '''Map the framework column on the column associated with the standard name in the mapping passed.'''
        df = deepcopy(df_made)
        # df.rename(columns=, inplace=True) # map in one direction -> to standard
        weather_parameters_mapping_inv = {v: k for k, v in self.weather_parameters_mapping.items()}
        df.rename(columns=weather_parameters_mapping_inv, inplace=True) #map in the other one -> to the choosen one
        return df

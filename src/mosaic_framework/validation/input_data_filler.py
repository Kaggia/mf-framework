################################################################################
# Module:      input_data_filler.py
# Description: Data filler, it receives a list of policies warning and data source, 
#              based on that fills the dataset returning it.
# Author:      Stefano Zimmitti, Lorenzo Gini
# Date:        21/08/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING, Any
import pandas as pd


if TYPE_CHECKING:
    from mosaic_framework.validation.filling_policies import DataFillingPolicy
    from mosaic_framework.data_storage.variable import SharedVariable

class InputDataFiller():
    '''
    Entry point to filling the data accordinglt with the policy chooosen
    '''
    def __init__(self, weather_parameters_mapping, **kwargs) -> None:
        self.coordinates                = self.get_coordinates(kwargs.get('coordinates', None))
        self.weather_parameters_mapping = weather_parameters_mapping
        self.version                    = kwargs.get('version', 1)

    def get_coordinates(self, coords:SharedVariable)->Any:
        """We get the coordinates if a SharedVariable is inplace.

        Args:
            coords (SharedVariable): Coordinates if SharedVariable is inplace. Otherwise None.

        Returns:
            Dict | None: {'latitude': <float>, 'longitude': <float>} | None
        """
        return coords.content if coords != None else None
    
    def fill(self, data:pd.DataFrame, missing_data:dict)->pd.DataFrame:
        '''Method used to fill the missing entry with his respectively policy.'''
        #Example of missing_data content:
        #{}
        data_filled = data
        
        # Filling data with the policy choosen, got from the InputDataValidator
        for missing_point in missing_data['missing_data_points']:
            date           = list(missing_point.keys())[0] # extract missing date
            filling_policy: DataFillingPolicy = missing_point[date](day=date, weather_parameters_mapping = self.weather_parameters_mapping, 
                                                                    version=self.version,coordinates=self.coordinates)
            data_filled    = filling_policy.apply(data=data_filled, 
                granularity=missing_data['details']['granularity'],
                dt_column = missing_data['details']['dt_column'], 
                )
        # data_filled[self.missing_date['details']['dt_column']] = data_filled[self.missing_date['details']['dt_column']].apply(lambda x: x.strftime(format='%Y-%m-%d %H:%M'))
        
        return data_filled \
            if isinstance(data_filled, pd.DataFrame) \
            else data
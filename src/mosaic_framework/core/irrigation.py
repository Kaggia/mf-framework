################################################################################
# Module: irrigation.py
# Description: Implementation of irrigation factors, or related to irrigation.
# Author:     Stefano Zimmitti
# Date: 25/02/2025
# Company: xFarm Technologies
################################################################################

from typing import List, Dict, Any
import pandas as pd
from copy import deepcopy

from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.exceptions import DataFormatException

class IrrigationDeficit(AgroRule):
    """
    This class represents a generic comparative rule.
    It takes a target column, a column to compare, and a condition.
    It applies the condition to the target column and returns a new dataframe with the result.

    Args:
        parameter (str): Parameter to extract from phenological stage data
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """

    def __init__(self, previous_data_index:int, taw:float, raw:float, irrigation_coefficient:float, default_value:float=0, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.previous_data_index     = previous_data_index
        self.taw                     = taw
        self.raw                     = raw
        self.irrigation_coefficient  = irrigation_coefficient
        self.default_value           = default_value
    
    def validate_parameters(self, data: pd.DataFrame) -> List[str]:
        """
        Get the parameters for the irrigation rule.
        """
        parameters = []
        
        # For each target, find columns with _prec_N suffix up to previous_data_index
        for target in self.target:            
            # Add columns with _previous_ suffix and verify they exist
            for i in range(1, self.previous_data_index + 1):
                previous_col = f"{target}_previous_{i}"
                if previous_col not in data.columns:
                    raise DataFormatException(f"Previous data column {previous_col} not found in data")
                parameters.append(previous_col)
        return parameters
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the ActualPhenostage rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe to process

        Returns:
            pd.DataFrame: Processed dataframe with phenological stage data added
        """
        def calculate_irration_deficit(data: pd.DataFrame) -> pd.Series:
            # Initialize empty list to store results with same length as input data
            di         = [self.default_value] * len(data)
            etc_prev_1 = data["etc_previous_1"].values
            etc_prev_2 = data["etc_previous_2"].values
            pu_prev_1  = data["pu_previous_1"].values
            pu_prev_2  = data["pu_previous_2"].values
            Rain_sum   = data["Rain_sum"].values
            fase_in    = data["fase_in"].values
            for i in range(0, len(di)): #per tutte le righe
                if i >= self.previous_data_index:
                    # Calculate the irrigation deficit
                    if self.taw - di[i-1] > self.raw:
                        di[i] = 1
                    else:
                        di[i] = min((self.taw - di[i-1])/(self.taw-self.raw), 1)
                    etr            = di[i] * etc_prev_1[i] #ETr
                    if di[i-1] < 0:
                        partial_factor1 = etr-pu_prev_1[i]
                    else:
                        partial_factor1 = etr-pu_prev_1[i]+di[i-1]
                    partial_factor = partial_factor1 * 1
                    if self.taw - di[i-1] > 5:
                        if fase_in[i-1] > 0 :
                            if max(min(self.raw-di[i-1], self.raw), 0) == 0 and Rain_sum[i] < 5:
                                if self.taw - di[i-2] > self.raw:
                                    di[i] = 1
                                else:
                                    di[i] = min((self.taw - di[i-2])/(self.taw-self.raw), 1)
                                    etr_2 = di[i] * etc_prev_2[i]
                                    partial_factor_2 = etr_2-pu_prev_2[i]+di[i-2]
                                    irrigation = partial_factor_2*self.irrigation_coefficient
                            else:
                                irrigation = 0
                        else:
                            irrigation = 0
                    else:
                        irrigation = self.taw / 2
                    di[i] = partial_factor - irrigation
            return di

        AgroRule.evaluate(self, data=data)
        data    = self.prepare(data)
        updt_df = deepcopy(data)

        self.validate_parameters(data=data)
        #get the actual phase data parameter, 
        #by selecting the phase that contains the gdd value
        updt_df[self.column] = calculate_irration_deficit(data=data)

        return self.finalize(data=updt_df)
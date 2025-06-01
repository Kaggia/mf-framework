################################################################################
# Module: output_factors.py
# Description: Implementation of grouping_up rules, used in output estimate
#   to extract from a side dataframe all info about.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, Callable, TYPE_CHECKING, Dict, Any

import re
import time
import numpy as np
import pandas as pd
from copy import deepcopy
import datetime

if TYPE_CHECKING:
    from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
    
    MosaicRulesHubType  = MosaicRulesHub

from mosaic_framework.core.math_utils import get_mapped_function
from mosaic_framework.core.agronomical_factors import ProtocolAgroRule
from mosaic_framework.core.functions import apply_condition, apply_condition_over_values
from mosaic_framework.core.exceptions import DataFormatException
from mosaic_framework.dt.datetime_parser import DatetimeParser

class OutputAgroRule(ProtocolAgroRule):
    """
    OutputAgroRule inherits from ProtocolAgroRule and implements prepare, evaluate, and finalize methods.
    Allows selecting a max of a single day in the selected column, then comparing it to a threshold.

    Parameters:
        column (str): Column name for the agronomical factor
        target (str): Target column name 
        ref (int): Reference value for comparison
        debug (bool): Whether to print debug information
    """
    def __init__(self, column: str, target: str, ref: int, debug: bool) -> None:
        self.column = column
        self.target = self.get_target(target=target)
        self.ref    = ref
        self.debug  = debug
        self.start_time  = None
        self.end_time    = None
        self.rules_hub   = None

    def get_target(self, target: Any) -> List[str]:
        """
        Gets the target column(s) name.

        Parameters:
            target: Target column name(s)

        Returns:
            List[str]: List of target column names
        """
        if isinstance(target, str):
            return [target]
        elif isinstance(target, list):
            return target
        else:
            raise ValueError(f"Target must be a string or a list of strings. Found: {type(target)}")

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares the data for rule evaluation.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        self.rules_hub.register(rule=self)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the rule on input data.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Evaluated data
        """
        if self.debug: print(f"Evaluating  : {self.column + ' '*(25-len(self.column))} | {str(type(self))[str(type(self)).rfind('.')+1:-2]}")
        self.start_time = time.time()
        return data
    
    def finalize(self, daily_data: pd.DataFrame, hourly_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Finalizes the rule evaluation.

        Parameters:
            daily_data: Daily aggregated DataFrame
            hourly_data: Hourly DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of finalized daily and hourly DataFrames
        """
        self.end_time = time.time()
        elab_time     = f"duration: {round(self.end_time-self.start_time, 4)} seconds."
        if self.debug: print(f"└Finished   : {self.column + ' '*(25-len(self.column))} | {elab_time} | {str(type(self))[str(type(self)).rfind('.')+1:-2]}\n")
        return daily_data, hourly_data
    
    def set_rules_hub(self, rules_hub: MosaicRulesHubType) -> None:
        """
        Sets the current MosaicRulesHub for the AgroRule.

        Parameters:
            rules_hub: MosaicRulesHub for the current session
        """
        self.rules_hub = rules_hub

#This Rule allows to select a max of a single day, in the 
#selected column, then compare it to a certain threshold
class SelectMaxAndCompare(OutputAgroRule):
    """
    OutputAgroRule that selects maximum value per day from a column and compares to a threshold.

    Parameters:
        column (str): Column name for the agronomical factor
        target (str): Target column name
        ref (int): Reference value for comparison
        debug (bool): Whether to print debug information
        condition (str): Condition for rule evaluation
    """

    def __init__(self, condition: str, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None) , ref=kwargs.get('ref', 0), debug=kwargs.get('debug', False))
        self.condition = self.get_condition(condition)
    
    def get_condition(self, raw_condition: str) -> Dict[str, Any]:
        """
        Parses raw condition string to extract comparison prefix and numerical value.

        Parameters:
            raw_condition: Raw condition string (e.g. 'gt0', 'lt0')

        Returns:
            Dict[str, Any]: Dictionary with comparison prefix and value
        """
        # Extract the comparison prefix (letters before the number)
        comp = re.sub(r'-?\d+(\.\d+)?$', '', raw_condition)
        
        # Extract the numerical value (supporting negatives)
        val_match = re.search(r'-?\d+(\.\d+)?$', raw_condition)
        val = float(val_match.group()) if val_match else None

        return {'comp': comp, 'val': val}
    

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        data = super().prepare(data=data)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Evaluates rule on input data.

        Parameters:
            data: Input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of daily and hourly DataFrames with rule applied
        """
        prepared_data = self.prepare(data=data)
        OutputAgroRule.evaluate(self, data=prepared_data)
        updt_data     = deepcopy(prepared_data)

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(updt_data)
        daily_data['date'] = np.nan
        daily_data['date'] = daily_data['sampleDate'].apply(lambda x: x[:x.index(" ")])
        
        if isinstance(self.target, list):
            self.target = self.target[0]
        
        #Grouping up data based on sample_date (date part)
        aggregate_functions = {self.target: ['max'], }
        map_output_columns  = {f'{self.target}_max':'max_inner_column'}

        grouped_multiple = \
            daily_data.groupby(['date']).agg(aggregate_functions)
        
        merged_cols = ['_'.join(multicol) for multicol in grouped_multiple.columns]
        grouped_multiple.columns=grouped_multiple.columns.droplevel(1)
        for i, _ in enumerate(merged_cols):
            merged_cols = list(map(lambda x: x.replace(merged_cols[i], map_output_columns[merged_cols[i]]), merged_cols))
        grouped_multiple.columns=merged_cols
        grouped_multiple.reset_index(inplace=True)  
        grouped_multiple = grouped_multiple.to_dict('records')
        for i, _ in enumerate(grouped_multiple):
            grouped_multiple[i]['sampleDate'] = grouped_multiple[i]['date'] + " 00:00"
            del grouped_multiple[i]['date']
        
        grouped_multiple = pd.DataFrame(grouped_multiple)

        #Applying the core check
        grouped_multiple[self.column] = grouped_multiple['max_inner_column'].apply(lambda x:apply_condition(x, cond=self.condition))
        grouped_multiple[self.column] = grouped_multiple[self.column].shift(self.ref).fillna(0.0)
        #Drop mid-result columns
        grouped_multiple.drop('max_inner_column', axis=1, inplace=True)

        #Also we need to dump result, with different granularity into hourly data.
        merged_df = pd.merge(updt_data, grouped_multiple, on='sampleDate', how='left')
        merged_df[self.column].fillna(0.0, inplace=True)

        #remove column on presence, eventually
        if 'sampleDate_daily' in merged_df.columns:
            merged_df.drop(columns=['sampleDate_daily'], inplace=True)  

        return self.finalize(daily_data=grouped_multiple, hourly_data=merged_df)

#This Rule allows to select a max of a single day, in the 
#selected column, then compare it to a certain threshold
class SelectMaxApplyAndComparison(OutputAgroRule):
    """
    OutputAgroRule that selects maximum values per day from multiple columns and compares them simultaneously to thresholds.

    Parameters:
        column (str): Column name for the agronomical factor
        target (str): Target column name
        ref (int): Reference value for comparison
        debug (bool): Whether to print debug information
        condition (str): Condition for rule evaluation
    """

    def __init__(self, condition: str, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None) , ref=kwargs.get('ref', 0), debug=kwargs.get('debug', False))
        self.condition = self.get_condition(condition)
    
    def get_condition(self, raw_condition: str) -> Dict[str, Any]:
        """
        Parses raw condition string to extract comparison prefix and numerical value.

        Parameters:
            raw_condition: Raw condition string (e.g. 'gt0', 'lt0')

        Returns:
            Dict[str, Any]: Dictionary with comparison prefix and value
        """
        # Extract the comparison prefix (letters before the number)
        comp = re.sub(r'-?\d+(\.\d+)?$', '', raw_condition)
        
        # Extract the numerical value (supporting negatives)
        val_match = re.search(r'-?\d+(\.\d+)?$', raw_condition)
        val = float(val_match.group()) if val_match else None

        return {'comp': comp, 'val': val}
    

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        data = super().prepare(data=data)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Evaluates rule on input data.

        Parameters:
            data: Input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of daily and hourly DataFrames with rule applied
        """
        data          = OutputAgroRule.evaluate(self, data=data)
        prepared_data = self.prepare(data=data)
        updt_data     = deepcopy(prepared_data)

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(updt_data)
        daily_data['date'] = np.nan
        daily_data['date'] = daily_data['sampleDate'].apply(lambda x: x[:x.index(" ")])

        #Grouping up data based on sample_date (date part)
        
        #aggregate_functions = {self.target: ['max']}
        aggregate_functions = {t:['max'] for t in self.target}

        #map_output_columns  = {f'{self.target}_max':'max_inner_column'}
        map_output_columns  =  {f'{t}_max': 'max_inner_column_'+t for t in self.target}

        grouped_multiple = \
            daily_data.groupby(['date']).agg(aggregate_functions)
                
        merged_cols = ['_'.join(multicol) for multicol in grouped_multiple.columns]
        grouped_multiple.columns=grouped_multiple.columns.droplevel(1)
        new_merged_cols = [map_output_columns[col] if col in map_output_columns else col for col in merged_cols]
        merged_cols     = new_merged_cols
        grouped_multiple.columns=merged_cols
        grouped_multiple.reset_index(inplace=True)  
        grouped_multiple = grouped_multiple.to_dict('records')
        for i, _ in enumerate(grouped_multiple):
            grouped_multiple[i]['sampleDate'] = grouped_multiple[i]['date'] + " 00:00"
            del grouped_multiple[i]['date']
        
        grouped_multiple = pd.DataFrame(grouped_multiple)
        
        and_columns                   = map_output_columns.values()
        grouped_multiple[self.column] = grouped_multiple.apply(lambda row:apply_condition_over_values(v=[row.to_dict()[k] for k in row.to_dict() if k in and_columns], cond=self.condition, iterable_fnc=all), axis=1)

        grouped_multiple[self.column] = grouped_multiple[self.column].shift(self.ref).fillna(0.0)
        #Applying the core check
        for c in merged_cols:
            print(f"removing: {c}")
            #Drop mid-result columns
            grouped_multiple.drop(c, axis=1, inplace=True)

        #Also we need to dump result, with different granularity into hourly data.
        merged_df = pd.merge(updt_data, grouped_multiple, on='sampleDate', how='left')
        merged_df[self.column].fillna(0.0, inplace=True)

        #remove column on presence, eventually
        if 'sampleDate_daily' in merged_df.columns:
            merged_df.drop(columns=['sampleDate_daily'], inplace=True)  

        return self.finalize(daily_data=grouped_multiple, hourly_data=merged_df)

#This Rule allows to Window the data
class ApplyWindowing(OutputAgroRule):
    """
    OutputAgroRule that applies windowing to data using specified functions.

    Parameters:
        column (str): Column name for the agronomical factor
        target (str): Target column name
        window_past (int): Window size for past values
        window_current (int): Window size for current values
        window_future (int): Window size for future values
        window_fnc (str): Mathematical function for windowing
        select_fnc (str): Mathematical function for data selection
        debug (bool): Whether to print debug information
    """

    def __init__(self, select_fnc: str, window_fnc: str, window_past: int, window_current: int, window_future: int, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None) , ref=kwargs.get('ref', 0), debug=kwargs.get('debug', False))
        self.window_past    = window_past
        self.window_current = window_current
        self.window_future  = window_future
        self.window_fnc     = get_mapped_function(fnc=window_fnc)
        self.select_fnc     = select_fnc


    def get_current_granularity(self, data: pd.DataFrame) -> str:
        """
        Gets current data granularity.

        Parameters:
            data: Input DataFrame

        Returns:
            str: Granularity as string
        """
        #Get the parser
        dt_parser = DatetimeParser()

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(data)
        daily_data['date']      = np.nan
        daily_data['date']      = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['timestamp'] = daily_data['date'].apply(lambda x: datetime.datetime.strptime(x, dt_parser.output_format).timestamp())
        avg_differences         = get_mapped_function('mean')([daily_data['timestamp'].to_list()[i+1]-daily_data['timestamp'].to_list()[i] for i in range(len(daily_data['timestamp'].to_list())-1)])
        #Getting the timeframe supported
        supported_timeframe      = {'daily': 86400, 'hourly':3600}
        supported_timeframe_copy = deepcopy(supported_timeframe)
        supported_timeframe      = {value: key for key, value in supported_timeframe_copy.items()}

        return supported_timeframe.get(avg_differences, None)
    
    def get_compact_data(self, data: pd.DataFrame, fnc: Callable) -> pd.DataFrame:
        """
        Compacts data using specified function.

        Parameters:
            data: Data to be compacted
            fnc: Function used for compacting

        Returns:
            pd.DataFrame: Compacted data
        """
        #Get the parser
        dt_parser = DatetimeParser()

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(data)
        daily_data['date'] = np.nan
        daily_data['date'] = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['date'] = daily_data['date'].apply(lambda x: x[:x.index("T")])
        
        if isinstance(self.target, list):
            self.target = self.target[0]
        
        #Grouping up data based on sample_date (date part)
        aggregate_functions = {self.target: [str(fnc)], }
        map_output_columns  = {f'{self.target}_{str(fnc)}':f'{str(fnc)}_inner_column'}

        grouped_multiple = \
            daily_data.groupby(['date']).agg(aggregate_functions)
        
        merged_cols = ['_'.join(multicol) for multicol in grouped_multiple.columns]
        grouped_multiple.columns=grouped_multiple.columns.droplevel(1)
        for i, _ in enumerate(merged_cols):
            merged_cols = list(map(lambda x: x.replace(merged_cols[i], map_output_columns[merged_cols[i]]), merged_cols))
        grouped_multiple.columns=merged_cols
        grouped_multiple.reset_index(inplace=True)  
        grouped_multiple = grouped_multiple.to_dict('records')
        for i, _ in enumerate(grouped_multiple):
            grouped_multiple[i]['sampleDate'] = grouped_multiple[i]['date'] + " 00:00"
            del grouped_multiple[i]['date']
        
        grouped_multiple = pd.DataFrame(grouped_multiple)
        grouped_multiple[self.target] = grouped_multiple[f'{str(fnc)}_inner_column']
        grouped_multiple.drop(f'{str(fnc)}_inner_column', axis=1)
        
        return grouped_multiple
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        data = super().prepare(data=data)

        #replacing target, this rule support one column as a target
        self.target = self.target[0] if isinstance(self.target, list) else self.target

        #Need to validate a bit the window data provided
        if not self.window_current in [0, 1]:
            raise DataFormatException
        
        return data
    
    def evaluate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Evaluates rule on input data.

        Parameters:
            data: Input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of daily and hourly DataFrames with rule applied
        """
        def apply_fnc(column_data: List[float], window: tuple) -> List[float]:
            """
            Applies moving average on custom window with past, current and future points.

            Parameters:
                column_data: Data to apply window to
                window: Tuple of (past, current, future) window sizes

            Returns:
                List[float]: Windowed results
            """
            result = list()
            i = 0
            while i < len(column_data):
                # Check if current index is within valid window range
                has_valid_window = (i <= len(column_data) - window[2] - 1)
                
                if not has_valid_window:
                    result.append(0)
                    i += 1
                    continue
                    
                window_values = []
                
                # Add past values
                if window[0] > 0:
                    past_start = max(0, i - window[0])  # Ensure we don't go below 0
                    past_end = i
                    window_values.extend(column_data[past_start:past_end])
                
                # Add current value 
                if window[1] > 0:
                    window_values.append(column_data[i])
                    
                # Add future values
                if window[2] > 0:
                    future_start = i + 1
                    future_end = min(i + window[2] + 1, len(column_data))  # Ensure we don't exceed array length
                    window_values.extend(column_data[future_start:future_end])
                
                # Calculate window function on collected values
                window_result = self.window_fnc(window_values[-window[0]:] if window[0] > 0 else window_values)  # Only use last window[0] values
                result.append(window_result)
                
                i += 1
            return result
        
        prepared_data = self.prepare(data=data)
        OutputAgroRule.evaluate(self, data=prepared_data)
        updt_data     = deepcopy(prepared_data)

        #Get current data granularity
        current_granularity = self.get_current_granularity(data=updt_data)
        if current_granularity == None:
            raise DataFormatException("Found 'granularity' is not compatible with the current model.")

        #Check if the granularity is different from the actual.
        #Actual could be hourly, but the output one is (at least for now)
        #always daily.
        model_granularity = self.rules_hub.get_variable(key='granularity', error_policy="pass", default='skip').content
        
        compact_data = self.get_compact_data(data=data, fnc=self.select_fnc) \
            if  current_granularity!='daily' \
            else updt_data
        
        compact_data[self.column] = apply_fnc(column_data=compact_data[self.target].tolist(), window=(self.window_past, self.window_current, self.window_future))

        return self.finalize(daily_data=compact_data, hourly_data=updt_data)

#This Rule allows to Window the data
class ApplySusceptibility(OutputAgroRule):
    """
    OutputAgroRule that applies susceptibility calculations to data.

    Parameters:
        column (str): Column name for the agronomical factor
        target (str): Target column name
        risk_cap (int): Maximum value for risk capping
        select_fnc (str): Mathematical function for data selection
        grouping_fnc (str): Mathematical function for summarizing window values
        debug (bool): Whether to print debug information
    """

    def __init__(self, select_fnc: str, grouping_fnc: Callable='sum', susceptibility_window: int=3, susceptibility_column: str='susceptibility', risk_cap: int=4, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None) , ref=kwargs.get('ref', 0), debug=kwargs.get('debug', False))
        self.select_fnc              = select_fnc
        self.risk_cap                = risk_cap
        self.susceptibility_modifier = susceptibility_column
        self.susceptibility_window   = susceptibility_window
        self.fnc = get_mapped_function(fnc=grouping_fnc)

    def get_current_granularity(self, data: pd.DataFrame) -> str:
        """
        Gets current data granularity.

        Parameters:
            data: Input DataFrame

        Returns:
            str: Granularity as string
        """
        #Get the parser
        dt_parser = DatetimeParser()

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(data)
        daily_data['date']      = np.nan
        daily_data['date']      = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['timestamp'] = daily_data['date'].apply(lambda x: datetime.datetime.strptime(x, dt_parser.output_format).timestamp())
        avg_differences         = get_mapped_function('mean')([daily_data['timestamp'].to_list()[i+1]-daily_data['timestamp'].to_list()[i] for i in range(len(daily_data['timestamp'].to_list())-1)])
        #Getting the timeframe supported
        supported_timeframe      = {'daily': 86400, 'hourly':3600}
        supported_timeframe_copy = deepcopy(supported_timeframe)
        supported_timeframe      = {value: key for key, value in supported_timeframe_copy.items()}

        return supported_timeframe.get(avg_differences, None)
    
    def get_compact_data(self, data: pd.DataFrame, fnc: Callable) -> pd.DataFrame:
        """
        Compacts data using specified function.

        Parameters:
            data: Data to be compacted
            fnc: Function used for compacting

        Returns:
            pd.DataFrame: Compacted data
        """
        #Get the parser
        dt_parser = DatetimeParser()

        #Separe sampleDate from sampleTime
        #groupUp for sampleDate, applying max function
        daily_data = deepcopy(data)
        daily_data['date'] = np.nan
        daily_data['date'] = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['date'] = daily_data['date'].apply(lambda x: x[:x.index("T")])
        
        if isinstance(self.target, list):
            self.target = self.target[0]
        
        #Grouping up data based on sample_date (date part)
        aggregate_functions = {self.target: [str(fnc)], }
        map_output_columns  = {f'{self.target}_{str(fnc)}':f'{str(fnc)}_inner_column'}

        grouped_multiple = \
            daily_data.groupby(['date']).agg(aggregate_functions)
        
        merged_cols = ['_'.join(multicol) for multicol in grouped_multiple.columns]
        grouped_multiple.columns=grouped_multiple.columns.droplevel(1)
        for i, _ in enumerate(merged_cols):
            merged_cols = list(map(lambda x: x.replace(merged_cols[i], map_output_columns[merged_cols[i]]), merged_cols))
        grouped_multiple.columns=merged_cols
        grouped_multiple.reset_index(inplace=True)  
        grouped_multiple = grouped_multiple.to_dict('records')
        for i, _ in enumerate(grouped_multiple):
            grouped_multiple[i]['sampleDate'] = grouped_multiple[i]['date'] + " 00:00"
            del grouped_multiple[i]['date']
        
        grouped_multiple = pd.DataFrame(grouped_multiple)
        grouped_multiple[self.target] = grouped_multiple[f'{str(fnc)}_inner_column']
        grouped_multiple.drop(f'{str(fnc)}_inner_column', axis=1)
        
        return grouped_multiple
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation.

        Parameters:
            data: Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        data = super().prepare(data=data)

        #replacing target, this rule support one column as a target
        self.target = self.target[0] if isinstance(self.target, list) else self.target
        
        return data
    
    def evaluate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Evaluates rule on input data.

        Parameters:
            data: Input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of daily and hourly DataFrames with rule applied
        """
        def get_applied_result(values: List[int], susceptibility_constant: int, window: int, fnc: Callable, risk_cap: int) -> List[float]:
            updt_values = list()
            for i in range(len(values)):
                if i <= window:
                    updt_values.append(values[i])
                else:
                    if susceptibility_constant > 0:
                        sum_v = fnc([v for v in values[i-window+1:i+1]])
                        updt_values.append(0 if sum_v == 0 else 1 if sum_v == 1 else 
                                        min(min(values[i-1], risk_cap) + susceptibility_constant, risk_cap))
                    else:
                        updt_values.append(min(max(values[i] + susceptibility_constant, 0), risk_cap))
            return updt_values
        
        prepared_data = self.prepare(data=data)
        OutputAgroRule.evaluate(self, data=prepared_data)
        updt_data     = deepcopy(prepared_data)

        #Get current data granularity
        current_granularity = self.get_current_granularity(data=updt_data)
        if current_granularity == None:
            raise DataFormatException("Found 'granularity' is not compatible with the current model.")

        #Check if the granularity is different from the actual.
        #Actual could be hourly, but the output one is (at least for now)
        #always daily.
        model_granularity = self.rules_hub.get_variable(key='granularity', error_policy="pass", default='skip').content
        
        compact_data = self.get_compact_data(data=data, fnc=self.select_fnc) \
            if  current_granularity!='daily' \
            else updt_data
        
        #Looking for the susceptibility constant
        susceptibility_constant = updt_data[self.susceptibility_modifier].unique().tolist()[0]
        compact_data[self.column]    = get_applied_result(
            values=compact_data[self.target].to_list(),
            susceptibility_constant=susceptibility_constant,
            window=self.susceptibility_window,
            fnc=self.fnc, 
            risk_cap=self.risk_cap)

        return self.finalize(daily_data=compact_data, hourly_data=updt_data)

#This rule is used to copy the target column to the output column.
class SimpleOutputRule(OutputAgroRule):
    """
    Simple output rule that copies values from target column to output column.
    
    Parameters:
        select_fnc (str): Function to use for aggregating values when compacting data. Defaults to 'max'.
        **kwargs: Additional arguments passed to OutputAgroRule
            - column (str): Output column name
            - target (str): Input column name to copy values from
            - ref (int): Reference value, defaults to 0
            - debug (bool): Enable debug output, defaults to False
    """
    def __init__(self, select_fnc: str='max', **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None) , ref=kwargs.get('ref', 0), debug=kwargs.get('debug', False))
        self.select_fnc = select_fnc
    
    def get_current_granularity(self, data: pd.DataFrame) -> str:
        """
        Gets current data granularity by analyzing timestamp differences.

        Parameters:
            data (pd.DataFrame): Input DataFrame with sampleDate column

        Returns:
            str: Granularity as string ('daily' or 'hourly') or None if not recognized
        """
        # Get the parser
        dt_parser = DatetimeParser()

        # Parse dates and calculate average time difference between samples
        daily_data = deepcopy(data)
        daily_data['date']      = np.nan
        daily_data['date']      = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['timestamp'] = daily_data['date'].apply(lambda x: datetime.datetime.strptime(x, dt_parser.output_format).timestamp())
        avg_differences         = get_mapped_function('mean')([daily_data['timestamp'].to_list()[i+1]-daily_data['timestamp'].to_list()[i] for i in range(len(daily_data['timestamp'].to_list())-1)])
        
        # Map average difference to granularity
        supported_timeframe      = {'daily': 86400, 'hourly':3600}
        supported_timeframe_copy = deepcopy(supported_timeframe)
        supported_timeframe      = {value: key for key, value in supported_timeframe_copy.items()}

        return supported_timeframe.get(avg_differences, None)
    
    def get_compact_data(self, data: pd.DataFrame, fnc: Callable) -> pd.DataFrame:
        """
        Compacts data by aggregating values per day using the specified function.

        Parameters:
            data (pd.DataFrame): Data to be compacted
            fnc (Callable): Function used for aggregating values

        Returns:
            pd.DataFrame: Compacted data with daily granularity
        """
        # Get the parser and prepare data
        dt_parser = DatetimeParser()
        daily_data = deepcopy(data)
        daily_data['date'] = np.nan
        daily_data['date'] = dt_parser.parse_batch(daily_data['sampleDate'].to_list())
        daily_data['date'] = daily_data['date'].apply(lambda x: x[:x.index("T")])
        
        # Handle single target column
        if isinstance(self.target, list):
            self.target = self.target[0]
        
        # Set up aggregation
        aggregate_functions = {self.target: [str(fnc)], }
        map_output_columns  = {f'{self.target}_{str(fnc)}':f'{str(fnc)}_inner_column'}

        # Group and aggregate data
        grouped_multiple = \
            daily_data.groupby(['date']).agg(aggregate_functions)
        
        # Clean up column names
        merged_cols = ['_'.join(multicol) for multicol in grouped_multiple.columns]
        grouped_multiple.columns=grouped_multiple.columns.droplevel(1)
        for i, _ in enumerate(merged_cols):
            merged_cols = list(map(lambda x: x.replace(merged_cols[i], map_output_columns[merged_cols[i]]), merged_cols))
        grouped_multiple.columns=merged_cols
        grouped_multiple.reset_index(inplace=True)  
        
        # Convert to records and reformat dates
        grouped_multiple = grouped_multiple.to_dict('records')
        for i, _ in enumerate(grouped_multiple):
            grouped_multiple[i]['sampleDate'] = grouped_multiple[i]['date'] + " 00:00"
            del grouped_multiple[i]['date']
        
        # Final cleanup
        grouped_multiple = pd.DataFrame(grouped_multiple)
        grouped_multiple[self.target] = grouped_multiple[f'{str(fnc)}_inner_column']
        grouped_multiple.drop(f'{str(fnc)}_inner_column', axis=1)
        
        return grouped_multiple
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation by ensuring target is a single column.

        Parameters:
            data (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: Prepared data
        """
        data = super().prepare(data=data)

        # Replace target list with single column
        self.target = self.target[0] if isinstance(self.target, list) else self.target
        
        return data
    
    def evaluate(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Evaluates rule by copying target column values to output column.

        Parameters:
            data (pd.DataFrame): Input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple containing:
                - Daily aggregated DataFrame
                - Original hourly DataFrame
        """
        prepared_data = self.prepare(data=data)
        OutputAgroRule.evaluate(self, data=prepared_data)
        updt_data     = deepcopy(prepared_data)

        # Check data granularity
        current_granularity = self.get_current_granularity(data=updt_data)
        if current_granularity == None:
            raise DataFormatException("Found 'granularity' is not compatible with the current model.")
        
        # Compact if needed
        compact_data = self.get_compact_data(data=data, fnc=self.select_fnc) \
            if  current_granularity!='daily' \
            else updt_data
        
        # Copy target to output column
        compact_data[self.column] = compact_data[self.target]

        return self.finalize(daily_data=compact_data, hourly_data=updt_data)
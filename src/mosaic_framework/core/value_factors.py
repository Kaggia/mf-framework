################################################################################
# Module: value_factors.py
# Description: Implementation of value factors.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

import pandas as pd
from copy import deepcopy
from typing import List, Dict
from datetime import datetime
import json
from ast import literal_eval

from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.exceptions import AgroRuleFormatError

class Value(AgroRule):
    """
    Assigns a simple value to an entire column.
    ON_CONDITION FILTERING IS <NOT> ACTIVE

    Parameters:
        value: Value to assign to the column
        target: Target column name
        column: Column name for the agronomical factor
        is_implicit: Whether the column is implicit
        debug: Whether to print debug information
    """
    def __init__(self, value: float, **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None), column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.value = value

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation

        Parameters:
            data: Input DataFrame to prepare

        Returns:
            pd.DataFrame: Prepared dataset
        """
        prepared_dataset = super().prepare(data)
        return prepared_dataset
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the rule on the input data

        Parameters:
            data: Input DataFrame to evaluate

        Returns:
            pd.DataFrame: DataFrame with evaluated rule
        """
        AgroRule.evaluate(self, data=data)
        df = self.prepare(data)
        updt_df = deepcopy(df)
        updt_df[self.column] = self.value

        return self.finalize(data=updt_df)

class MappedValueOnTimeRangesRule(AgroRule):
    """
    Maps values based on time ranges.
    Mapping contains time ranges defined as:
    {'01-01 to 01-31': 1, '02-01 to 02-31': 2, '03-01 to 03-31': 3, 'default':0.0}

    Parameters:
        mapping: Dictionary containing time range mappings
        target: Target column name
        column: Column name for the agronomical factor
        on_condition: Condition for applying the rule
        is_implicit: Whether the column is implicit
        debug: Whether to print debug information
    """
    def __init__(self, mapping: Dict, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None), 
            column=kwargs.get('column', None), 
            on_condition=kwargs.get('on_condition', None), 
            is_implicit=kwargs.get('is_implicit', False), 
            debug=kwargs.get('debug', False))
        self.mapping = mapping
    
    def validation(self) -> bool:
        """
        Validates mapping format

        Returns:
            bool: True if validation passes
        """
        for k in list(self.mapping.keys()):
            if k != 'default':
                try:
                    int(k.split(" to ")[0].split("-")[0])
                    int(k.split(" to ")[0].split("-")[1])
                    int(k.split(" to ")[1].split("-")[0])
                    int(k.split(" to ")[1].split("-")[1])
                except Exception as e:
                    raise AgroRuleFormatError(f"Mapping key {k} is not well formatted. Error: {e}")
        return True

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation

        Parameters:
            data: Input DataFrame to prepare

        Returns:
            pd.DataFrame: Prepared dataset
        """
        prepared_dataset = super().prepare(data)
        self.validation()
        internal_df = deepcopy(prepared_dataset)
        internal_df['sampleDate_dt'] = pd.to_datetime(internal_df[self.target])
        
        ref_year = int(internal_df['sampleDate_dt'].min().year)
        updt_mapping = []
        for k in list(self.mapping.keys()):
            if k != 'default':
                start_date = datetime(ref_year, int(k.split(" to ")[0].split("-")[0]), int(k.split(" to ")[0].split("-")[1]))
                end_date = datetime(ref_year, int(k.split(" to ")[1].split("-")[0]), int(k.split(" to ")[1].split("-")[1]))

                if start_date > end_date:
                    end_date = end_date.replace(year=end_date.year+1)

                updt_mapping.append({
                    'start': start_date,
                    'end': end_date,
                    'value': self.mapping[k]
                })
            else:
                updt_mapping.append({
                    'start': "default",
                    'end': "default",
                    'value': self.mapping['default']
                })
        self.mapping = updt_mapping
        return prepared_dataset

    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the rule on the input data

        Parameters:
            data: Input DataFrame to evaluate

        Returns:
            pd.DataFrame: DataFrame with evaluated rule
        """
        def get_mapped_value(value, mapping):
            dt_value = pd.to_datetime(value)
            for m in mapping:
                if m['start'] != "default":
                    if dt_value >= m['start'] and dt_value <= m['end']:
                        return m['value']
            return [d['value'] for d in mapping if d['start']=='default' and d['end']=='default'][0]
        updt_df = AgroRule.evaluate(self, data=data)

        updt_df[self.column] = updt_df[self.target].apply(lambda x:get_mapped_value(x, mapping=self.mapping))

        return self.finalize(data=updt_df)

class ReferenceValue(Value):
    """
    Gets a past value from a selected target column.
    Sets default uncalculable values to 0.0.

    Parameters:
        target: Target column name
        ref: Number of rows to look back
        column: Column name for the agronomical factor
        is_implicit: Whether the column is implicit
        debug: Whether to print debug information
    """
    def __init__(self, target: str, ref: int, **kwargs) -> None:
        super().__init__(value=0, **kwargs)
        self.ref = ref
        self.target = target
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for rule evaluation

        Parameters:
            data: Input DataFrame to prepare

        Returns:
            pd.DataFrame: Prepared dataset
        """
        prepared_dataset = super().prepare(data)
        return prepared_dataset
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the rule on the input data

        Parameters:
            data: Input DataFrame to evaluate

        Returns:
            pd.DataFrame: DataFrame with evaluated rule
        """
        AgroRule.evaluate(self, data=data)
        df = self.prepare(data)
        updt_df = deepcopy(df)

        values = updt_df[self.target].to_list()
        for _ in range(self.ref):
            values.pop()
        default_values = [0.0 for i in range(self.ref)]
        updt_df[self.column] = default_values + values
        return self.finalize(data=updt_df)

class MapValuesRule(AgroRule):
    """
    Maps values according to a predefined mapping dictionary.

    Parameters:
        mapping: Dictionary containing value mappings
        target: Target column name
        column: Column name for the agronomical factor
        on_condition: Condition for applying the rule
        is_implicit: Whether the column is implicit
        debug: Whether to print debug information
    """
    def __init__(self, mapping: Dict, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None), 
            column=kwargs.get('column', None), 
            on_condition=kwargs.get('on_condition', None), 
            is_implicit=kwargs.get('is_implicit', False), 
            debug=kwargs.get('debug', False))
        self.mapping = self.parse_mapping(mapping)
    
    def parse_mapping(self, mapping: str) -> Dict:
        """
        Parses a mapping dictionary from string format to Python dict

        Parameters:
            mapping: String representation of mapping dictionary or Dict

        Returns:
            Dict: Parsed mapping dictionary
        """
        if isinstance(mapping, dict):
            return mapping
        elif isinstance(mapping, str):
            # Use ast.literal_eval instead of json.loads for Python dict strings
            literal_mapping = literal_eval(mapping)
            literal_mapping = {float(k) if str(k).replace('.','').isdigit() else str(k): v for k,v in literal_mapping.items()}
            return literal_mapping

    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the rule on the input data

        Parameters:
            data: Input DataFrame to evaluate

        Returns:
            pd.DataFrame: DataFrame with evaluated rule
        """
        def get_mapped_value(val: object, mapping: Dict) -> float:
            return mapping[val] \
                if val in list(mapping.keys()) \
                else mapping['default']
        
        updt_df = AgroRule.evaluate(self, data=data)
        
        updt_df[self.column] = updt_df[self.target].apply(lambda x:get_mapped_value(val=x, mapping=self.mapping))

        return self.finalize(data=updt_df)

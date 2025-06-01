################################################################################
# Module: reflection_factors.py
# Description: Implementation of reflective factors.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

import re
#from sympy import sympify
import pandas as pd
from copy import deepcopy
from typing import List, Callable, Dict
import numpy as np

from mosaic_framework.core.functions import apply_condition
from mosaic_framework.core.agronomical_factors import AgroRule

#Base class to group up all common reflective behaviours 
class ReflectiveAgroFactor(AgroRule):
    """
    ReflectiveAgroFactor is a base class for reflective factors.
    It inherits from AgroRule and provides common functionality for reflective factors.

    Parameters:
        ref: The reference value for comparison
        target: The target column name
        column: The column name for the agronomical factor
        is_implicit: Whether the column is implicit or not
        debug: Whether to print debug information or not
    """
    def __init__(self, ref:object, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None), 
            column=kwargs.get('column', None), 
            is_implicit=True, 
            debug=False)
        self.ref    = ref

#Describe a particular Rule, where it returns a value, based
#on actual index, and the shift. Used to point to a value, 
#that is contained in a column, where calculations must be
#made for old-data, already calculated.
#WARN: is_implicit is set to TRUE, so a column name (random)
#is produced, maybe we can avoid that?
class ReflectiveValue(ReflectiveAgroFactor):
    """
    ReflectiveValue is a class that represents a reflective value factor.
    It inherits from ReflectiveAgroFactor and provides functionality to evaluate a reflective value.
    
    Parameters:
        ref: The reference value for comparison
        target: The target column name
        column: The column name for the agronomical factor
        is_implicit: Whether the column is implicit or not
        debug: Whether to print debug information or not
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None),
            ref=kwargs.get('ref', None),
            column=kwargs.get('column', None), 
            is_implicit=True, 
            debug=False)
    
    def evaluate(self, data:pd.DataFrame, actual_index:int) -> float:
        """
        Evaluate the rule on the input data.
        It returns the value of the target column at the specified actual index,
        shifted by the ref value.

        Parameters:
            data: Input DataFrame to evaluate
            actual_index: Current index position

        Returns:
            float: Value at the specified index position
        """
        return data[self.target][actual_index-self.ref:actual_index-self.ref+1].values[0]

    def __str__(self) -> str:
        return f"class: {self.__class__} | target={self.target} | ref={self.ref}"

#Add a simple check on condition for a reflective value, 
#first get the value than compare it to a condition 'gt-like'
class ReflectiveCondition(ReflectiveAgroFactor):
    """
    ReflectiveValue is a class that represents a reflective value factor.
    It inherits from ReflectiveAgroFactor and provides functionality to evaluate a reflective value.
    
    Parameters:
        ref: The reference value for comparison
        target: The target column name
        column: The column name for the agronomical factor
        is_implicit: Whether the column is implicit or not
        debug: Whether to print debug information or not
        condition: The condition for the reflective value factor
    """

    def __init__(self, condition:str, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None),
            ref=kwargs.get('ref', None),
            column=kwargs.get('column', None), 
            is_implicit=True, 
            debug=False)
        self.condition = self.get_condition(raw_condition=condition)
    
    def get_condition(self, raw_condition: str) -> Dict[str, float]:
        """
        Parse the raw condition string to extract comparison prefix and numerical value.
        
        Parameters:
            raw_condition: String containing condition in format like 'gt0', 'lt0'

        Returns:
            Dict[str, float]: Dictionary with comparison operator and value
        """
        # Extract the comparison prefix (letters before the number)
        comp = re.sub(r'-?\d+(\.\d+)?$', '', raw_condition)
        
        # Extract the numerical value (supporting negatives)
        val_match = re.search(r'-?\d+(\.\d+)?$', raw_condition)
        val = float(val_match.group()) if val_match else None

        return {'comp': comp, 'val': val}
    

    def apply_condition(self, v:float, cond:Dict[str, float]) -> int:
        """
        Apply condition to value v.

        Parameters:
            v: Value to check condition against
            cond: Dictionary containing comparison operator and value

        Returns:
            int: 1 if condition is met, 0 otherwise
        """
        operators = {
                'gt'  :  v >  cond['val'],
                'goet':  v >= cond['val'],
                'lt'  :  v <  cond['val'],
                'loet':  v <= cond['val'],
                'et'  :  v == cond['val']
            }

        return 1 if operators[cond['comp']] else 0

    def evaluate(self, data:pd.DataFrame, actual_index:int) -> int:
        """
        Evaluate the rule on the input data.

        Parameters:
            data: Input DataFrame to evaluate
            actual_index: Current index position

        Returns:
            int: Result of applying condition to the value
        """
        return self.apply_condition(data[self.target].values[actual_index-self.ref:actual_index-self.ref+1], self.condition)

#<IMPLEMENT> <CHECK>
#Allows referring to a series of data (a slice) backwards
#like from 10 to 1 backwards.
class ReflectiveSeries(ReflectiveAgroFactor):
    """
    ReflectiveSeries is a class that represents a reflective series factor.
    It inherits from ReflectiveAgroFactor and provides functionality to evaluate a reflective series.
    It is used to apply an aggregation function to a timeframe and compare the result to a condition.    
    
    Parameters:
        ref: The reference value for comparison
        target: The target column name
        column: The column name for the agronomical factor
        is_implicit: Whether the column is implicit or not
        debug: Whether to print debug information or not
        condition: The condition for the reflective value factor
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.ref_start    = self.ref[0]
        self.ref_end      = self.ref[1]
        del self.ref
    
    def evaluate(self, data:pd.DataFrame, actual_index:int) -> np.ndarray:
        """
        Evaluate the rule on the input data.

        Parameters:
            data: Input DataFrame to evaluate
            actual_index: Current index position

        Returns:
            np.ndarray: Array of values from the specified range
        """
        return data[self.target][actual_index-self.ref_start:actual_index-self.ref_end+1].values

    def __str__(self) -> str:
        return f"class: {self.__class__} | target={self.target} | ref={self.ref}"

#Allows to get a series of data, considering as references the older
#ref starting from ref[0] to ref[1], apply aggregation_fnc and 
#confronting the result with the condition 
#ex. 1. va a vedere nelle 24 precedenti, 
    #2. trova il massimo, 
    #3. lo confronta, 
    #4. se è maggiore di 1 torna 1 altrimenti 0
class ReflectiveTimeframeCondition(ReflectiveSeries):
    """
    ReflectiveTimeframeCondition is a class that inherits from ReflectiveSeries.
    It is used to apply an aggregation function to a timeframe and compare the result to a condition.

    Parameters:
        ref: The reference value for comparison
        target: The target column name
        column: The column name for the agronomical factor
        is_implicit: Whether the column is implicit or not
        debug: Whether to print debug information or not
        fnc: The aggregation function to apply to the timeframe
        condition: The condition for the reflective value factor
    """
    def __init__(self, aggregation_fnc:Callable, condition:str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.fnc = aggregation_fnc
        self.condition = self.get_condition(raw_condition=condition)

    def get_condition(self, raw_condition: str) -> Dict[str, float]:
        """
        Parse the raw condition string to extract comparison prefix and numerical value.
        
        Parameters:
            raw_condition: String containing condition in format like 'gt0', 'lt0'

        Returns:
            Dict[str, float]: Dictionary with comparison operator and value
        """
        # Extract the comparison prefix (letters before the number)
        comp = re.sub(r'-?\d+(\.\d+)?$', '', raw_condition)
        
        # Extract the numerical value (supporting negatives)
        val_match = re.search(r'-?\d+(\.\d+)?$', raw_condition)
        val = float(val_match.group()) if val_match else None

        return {'comp': comp, 'val': val}
    

    def evaluate(self, data:pd.DataFrame, actual_index:int) -> int:
        """
        Evaluate the rule on the input data.

        Parameters:
            data: Input DataFrame to evaluate
            actual_index: Current index position

        Returns:
            int: Result of applying aggregation and condition
        """
        #Calling the evalute on ReflectiveSeries, obtaining the values (reflective)
        values = super().evaluate(data=data, actual_index=actual_index)
        #Here we need to apply aggregation_fnc 
        #also confrontation to condition, returning the result
        return apply_condition(self.fnc(values), cond=self.condition)

################################################################################
# Module: equation_factors.py
# Description: Implementation of equation factors.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

import re
from typing import List
import pandas as pd
from copy import deepcopy
from typing import Callable, Iterable, Dict
import numpy as np
from statistics import mean, median
from math import sin, cos, tan
from math import asin as arcsin, acos as arccos, atan as arctan
from numpy import sign 

from mosaic_framework.core.math_utils import get_mapped_function
from mosaic_framework.core.reflection import ReflectiveAgroRule
from mosaic_framework.core.reflection_factors import ReflectiveValue
from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.exceptions import ComplexValueError

#super class needed to define calculation on particular condition
#condition can be over an existing column | on demand AgroRule
#With sympy, we can dev evaluate function as a general case
#ON_CONDITION FILTERING IS ACTIVE
class Equation(AgroRule, ReflectiveAgroRule):
    """
    Equation is a class that inherits from AgroRule and ReflectiveAgroRule.
    It represents an equation that can be applied to a target column based on a condition.

    Args:
        target (str): The target column name
        column (str): The column name for the agronomical factor
        is_implicit (bool): Whether the column is implicit or not
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None
        debug (bool, optional): Whether to print debug information or not. Defaults to False
        apply (str): The equation to be applied
    """
    def __init__(self, apply:str, **kwargs) -> None:
        super().__init__(
            target=kwargs.get('target', None), 
            column=kwargs.get('column', None), 
            on_condition=kwargs.get('on_condition', None), 
            is_implicit=kwargs.get('is_implicit', False), 
            debug=kwargs.get('debug', False))
        self.target            = self.get_target_column(self.target)
        self.apply             = apply
        self.reflective_rules  = None

    def get_post_processed_equation(self, equation:str) -> str:
        """
        Replace special characters in the equation string.

        Args:
            equation (str): Original equation string

        Returns:
            str: Processed equation with replaced characters
        """
        equation = equation.replace('^', '**')
        equation = equation.replace('CONSTANT_E', f'{np.e}')
        return equation

    def get_on_condition_column(self) -> tuple:
        """
        Get the column name targeted by the on_condition.

        Returns:
            tuple: Column name and type (str or AgroRule)
        """
        col = None
        #targeting the column name
        if isinstance(self.on_condition, str):
            col = self.on_condition
            _type = str
        elif isinstance(self.on_condition, AgroRule):
            col   = self.on_condition.column
            _type = AgroRule
            #check wether the condition has a nested reflection on a target
        else:
            raise TypeError("target is not correct type. Must be: str | AgroRule")
        return col, _type

    def get_target_column(self, target:object) -> List:
        """
        Get the target column name(s).

        Args:
            target (object): Target specification as str, AgroRule or List

        Returns:
            List: List of target column names
        """
        col = None
        #targeting the column name
        if isinstance(target, str):
            col = [target]                     #convert list of one element
        elif isinstance(target, AgroRule):
            col = [target.column]              #convert list of one element
        elif isinstance(target, List):
            col = target                       #already a list
        else:
            raise TypeError("target is not correct type. Must be: str | AgroRule")
        return col

    def replace_target(self, values:Dict) -> str:
        """
        Replace target(s) in the equation with their corresponding values.

        Args:
            values (Dict): Dictionary mapping targets to values

        Returns:
            str: Equation with replaced target values
        """
        def contains_integer_in_brackets(key:str):
            return re.search(r'\[(-?\d+)\]', key)
        filled_equation = self.apply
        for k in self.target:
            updt_k = None
            if not contains_integer_in_brackets(key=k):
                updt_k = k
            else:
                updt_k = k[k.find("<")+1:k.find("[")]

            filled_equation = filled_equation.replace('<'+str(updt_k)+'>', str(values[updt_k]))
        return filled_equation
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for evaluation by handling reflection.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Prepared dataframe
        """
        def contains_integer_in_brackets(target_col:str):
            return re.search(r'\[(-?\d+)\]', target_col)

        #Auto-detect reflective rules, based on same target==column
        #target from inner rule, column from main rule.
        self.reflective_rules = list()

        #reflective columns are contained in 'self.target' and 'self.apply'
        #expressed as: column_name[-1] where [-1] is the older reference index
        #This kind of columns are not explicitated in the object instanciation
        #but are created when a Reflective column (with [-n]) is detected.
        #a reflective column column_name_ref_N will be created.
        
        for i, _ in enumerate(self.target):
            if contains_integer_in_brackets(self.target[i]):
                #ref_ref, reference with the brackets
                #ref_tar, is the target reference without brackets
                #ref_tar_res is the target reference with ref converted to "_N"
                ref_ref = int(self.target[i][self.target[i].find("[")+1:self.target[i].find("]")]) * -1
                ref_tar = self.target[i][self.target[i].find("<")+1:self.target[i].find("[")]
                self.reflective_rules.append(ReflectiveValue(target=ref_tar, ref=ref_ref, debug=True))
                #Also we need to take care of self.apply, 
                #cause it has column_name[-1] format
                self.apply = self.apply.replace('<'+self.target[i]+'>', '<'+ref_tar+'>')
        return super().prepare(data)

    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Processed dataframe with evaluated results
        """
        #on_condition is always a str
        def calculate_on_row(row:Dict, **kwargs):
            """
            Calculate the result for a given row by evaluating the equation.

            Args:
                row (Dict): Row data
                **kwargs: Additional arguments

            Returns:
                float: Calculated result
            """
            #this behaviour is tricky. Cause we want to use this function for 
            #reflection and normal use cases
            columns = kwargs.get('columns', self.target)
            #needs on_condition:str
            values = {tk:row[tk] for tk in columns}
            filled_equation   = self.replace_target(values=values)
            on_cond_column, _ = self.get_on_condition_column()
            #This line grants to filter (with 0 or 1) the equation just 
            #   evaluating equation with a 1
            #   filtering with a 0
            result = \
                eval(self.get_post_processed_equation(filled_equation)) \
                if row[on_cond_column] == 1 \
                else 0.0
            if str(result).find("I")!=-1 or str(result).find("j")!=-1:
                raise ComplexValueError(f"Evaluation returned a not handled complex number. filled_equation={filled_equation} | val={result}")
            return result        
        
        updt_df = AgroRule.evaluate(self, data=data)
        updt_df = self.prepare(updt_df)
                                
        #Effective evaluation starts as specified below for reflective case        
        if self.is_reflective:
            #We need to extract targets and pass them as involved_rules
            #Applying reflective rules, one by one
            #they have the same target.
            updt_df = self.reflective_evaluate(
                data=updt_df, 
                fnc=calculate_on_row,
                reflective_rules=self.reflective_rules,
                involved_columns=self.target, #We need to pass the targets WARN, they have BRACKETS!
                involved_condition=self.get_on_condition_column()[0],
                reflective_condition=self.on_condition \
                    if self.has_reflective_condition\
                    else None) 
                                     
            updt_df[self.column] = updt_df['reflective_'+self.column]
            updt_df.drop('reflective_'+self.column, axis=1, inplace=True)
        else:
            #then we define a new column - applying the logic row by row
            #but before that we checked the type and solved it if it is an AgroRule.
            #Has a different behaviour from Comparative rules, where an equation MUST
            #have all the factors all calculated before starting the effective evaluation.

            updt_df[self.column] = updt_df.apply(lambda row:calculate_on_row(row=row), axis=1)

        return self.finalize(data=updt_df)

#Class that allows to apply a single function to a set of columns
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class ApplyFunction(AgroRule):
    """
    ApplyFunction is a class that inherits from AgroRule. 
    Class that allows to apply a single function to a set of columns.
    
    Args:
        target (str): The target column name
        column (str): The column name for the agronomical factor
        is_implicit (bool): Whether the column is implicit or not
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None
        debug (bool, optional): Whether to print debug information or not. Defaults to False
        function (Callable): The function to be applied
    """
    def __init__(self, function:Callable, **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None),column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.function = get_mapped_function(function)
    
    def get_target_column(self, target:object) -> List:
        """
        Get the target column name(s).

        Args:
            target (object): Target specification as str, AgroRule or List

        Returns:
            List: List of target column names
        """
        col = None
        #targeting the column name
        if isinstance(target, str):
            col = [target]                     #convert list of one element
        elif isinstance(target, AgroRule):
            col = [target.column]              #convert list of one element
        elif isinstance(target, List):
            col = target                       #already a list
        else:
            raise TypeError("target is not correct type. Must be: str | AgroRule")
        return col
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Processed dataframe with evaluated results
        """
        AgroRule.evaluate(self, data=data)

        updt_df = deepcopy(data)

        #then we define a new column
        #applying self.function to each column specified in self.target
        updt_df[self.column] = updt_df.apply(lambda row:self.function([row.to_dict()[k] for k in list(row.to_dict().keys()) if k in self.target]), axis=1)

        return self.finalize(data=updt_df)

#Class that allows to apply a single function to subset of a column, 
#described by 'range' param, that estabilishes the indexes of start and end of
#the range.
class ApplyFunctionOnRange(AgroRule):
    """
    ApplyFunctionOnRange is a class that inherits from AgroRule. 
    Class that allows to apply a single function to subset of a column, 
    described by 'range' param, that establishes the indexes of start and end of
    the range.

    Args:
        target (str): The target column name
        column (str): The column name for the agronomical factor
        is_implicit (bool): Whether the column is implicit or not
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None
        debug (bool, optional): Whether to print debug information or not. Defaults to False
        function (Callable): The function to be applied
        range (Iterable): The range of indexes to apply the function on
        on_out_of_range (str): Policy to decide what to do on 'non present data'
            due to an index that is lesser than the range
    """
    def __init__(self, function:str, range:Iterable, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.function        = get_mapped_function(function)
        self.range           = range
        self.on_out_of_range = kwargs.get('on_out_of_range', 'default')
    
    def get_on_out_of_range_value(self, data:List[float], index:int, on_out_of_range:str) -> float:
        """
        If current index is out of range, fill based on policy.

        Args:
            data (List[float]): Data that are currently calculated
            index (int): Current index
            on_out_of_range (str): Policy to use

        Returns:
            float: Value based on policy
        """
        v = None
        if on_out_of_range == 'default':
            v = 0
        elif on_out_of_range == 'coerce':  
            v = self.function(data[0:index])
        return v

    def prepare(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for evaluation by handling reflection.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Prepared dataframe
        """
        prepared_dataset = super().prepare(data)

        return prepared_dataset
        
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Processed dataframe with evaluated results
        """
        AgroRule.evaluate(self, data=data)

        updt_df = deepcopy(data)

        ref_start = None
        ref_end   = None

        if isinstance(self.range, Iterable):
            if len(self.range) == 2:
                ref_start = self.range[0]
                ref_end   = self.range[1]
            else:
                raise ValueError(f"range is an iterable or at least a non-empty, max length of 2. Found: {self.range} of type: {type(self.range)}")
        else:
            ref_start = self.range
            ref_end   = 0

        #then we define a new column
        #applying self.function to each column specified in self.target
        values      = updt_df[self.target].to_list()
        updt_values = list()
        for i in range(len(values)):
            updt_values.append(self.function(values[i-ref_start:i-ref_end+1])if i>ref_start else self.get_on_out_of_range_value(data=values, index=i, on_out_of_range=self.on_out_of_range))
        
        updt_df[self.column] = updt_values
        return self.finalize(data=updt_df)

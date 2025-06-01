################################################################################
# Module: comparative_factors.py
# Description: Implementation of comparative factors.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

import re
import pandas as pd
from copy import deepcopy
from typing import List, Callable, Dict

from mosaic_framework.core.math_utils import get_mapped_function
from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.reflection import ReflectiveAgroRule
from mosaic_framework.core.reflection_factors import ReflectiveAgroFactor, ReflectiveSeries
from mosaic_framework.core.functions import and_rule_over_row, or_rule_over_row


#generic comparative rule
class ComparativeRule(AgroRule):
    """
    This class represents a generic comparative rule.
    It takes a target column, a column to compare, and a condition.
    It applies the condition to the target column and returns a new dataframe with the result.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        condition (str): Express the condition that must be met.
        boolean_mapping (Dict): A dictionary mapping the values of the target column to boolean values.
    """

    def __init__(self, condition:str="", **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None), column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.condition       = self.get_condition(raw_condition=condition) if condition!="" else ""
        self.boolean_mapping = kwargs.get('boolean_mapping', None)

    #get raw condition like: 'gt0', 'lt0', goet0, 'loet0' as:
    #{'comp': 'gt', val: 0}
    def get_condition(self, raw_condition: str) -> Dict:
        """
        This method parses the raw condition string to extract the comparison prefix
        and the numerical value (including negatives).
        
        Args:
            raw_condition (str): The raw condition string to parse
            
        Returns:
            Dict containing:
            - 'comp': the comparison prefix
            - 'val': the numerical value
        """
        # Extract the comparison prefix (letters before the number)
        comp = re.sub(r'-?\d+(\.\d+)?$', '', raw_condition)
        
        # Extract the numerical value (supporting negatives)
        val_match = re.search(r'-?\d+(\.\d+)?$', raw_condition)
        val = float(val_match.group()) if val_match else None

        return {'comp': comp, 'val': val}
    
    def to_actual_mapping(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        This method applies the boolean mapping to the input data.
        
        Args:
            data (pd.DataFrame): Input dataframe to apply mapping to
            
        Returns:
            pd.DataFrame with boolean mapping applied
        """
        updt_data = deepcopy(data)

        if self.boolean_mapping != None:
            updt_data[self.column] = updt_data[self.column].apply(lambda x:self.boolean_mapping[x])

        return updt_data

#This class rapresents the simpliest rule that can be applied.
#It maps rules like: 
#temperature > n || temperature < n || temperature == n
#it applies the logic over the entire column 'target'
#produces a pandas dataframe, composed by dates(index) and rule calculated 
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class SimpleComparativeRule(ComparativeRule):
    """
    This class rapresents the simpliest rule that can be applied.
    It maps rules like: 
    temperature > n || temperature < n || temperature == n
    it applies the logic over the entire column 'target'
    produces a pandas dataframe, composed by dates(index) and rule calculated.
    
    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        condition (str): Express the condition that must be met.
        boolean_mapping (Dict): A dictionary mapping the values of the target column to boolean values.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
            
    def prepare(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        This method prepares the input data for the evaluation.
        
        Args:
            data (pd.DataFrame): Input dataframe to prepare
            
        Returns:
            pd.DataFrame with target column prepared
        """
        prepared_dataset = super().prepare(data)
        return prepared_dataset
    
    #Evaluating the Rule, applying each time 'apply_condition' 
    #to the whole column selected as 'target'
    def evaluate(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        This method evaluates the rule on the input data.
        It applies the condition logic over the entire target column.
        
        Args:
            data (pd.DataFrame): Input dataframe to evaluate
            
        Returns:
            pd.DataFrame with rule applied
        """
        def apply_condition(v, cond:dict):
            operators = {
                    'gt'  :  v >  cond['val'],
                    'goet':  v >= cond['val'],
                    'lt'  :  v <  cond['val'],
                    'loet':  v <= cond['val'],
                    'et'  :  v == cond['val']
                }

            return 1 if operators[cond['comp']] else 0
        
        super().evaluate(data=data)
        df      = self.prepare(data)
        updt_df = deepcopy(df)
        
        updt_df[self.column] = updt_df[self.target].apply(lambda x:apply_condition(x, self.condition))
        
        updt_df = self.to_actual_mapping(data=updt_df)

        return self.finalize(data=updt_df)

#Compare a subset of a 'target' column (time sliced) against a condition
#It applies a rule that requires the dataset to be COMPLETE. This
#condition is reached before calculating the rule. During the validation
#phase.
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class ComparativeTimeframeRule(ComparativeRule):
    """
    Compare a subset of a 'target' column (time sliced) against a condition
    It applies a rule that requires the dataset to be COMPLETE. This
    condition is reached before calculating the rule. During the validation
    phase.  

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        condition (str): Express the condition that must be met.
        boolean_mapping (Dict): A dictionary mapping the values of the target column to boolean values.
        timeframe (int): The number of hours to consider for the comparison.
        aggregation_fnc (Callable): The aggregation function to apply to the target column.
    """

    def __init__(self, timeframe:int, aggregation_fnc:Callable, **kwargs) -> None:
        super().__init__(**kwargs)
        self.timeframe       = timeframe        #expressed as 'hours'
        self.aggregation_fnc = aggregation_fnc  #sum, mean, etc.
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        This method prepares the input data for the evaluation.
        
        Args:
            data (pd.DataFrame): Input dataframe to prepare
            
        Returns:
            pd.DataFrame with target column prepared
        """

        return super().prepare(data)
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        This method evaluates the rule on the input data.
        It applies the condition logic over the entire target column.
        
        Args:
            data (pd.DataFrame): Input dataframe to evaluate
            
        Returns:
            pd.DataFrame with rule applied
        """
        def apply_condition(v, cond:dict):
            operators = {
                    'gt'  :  v >  cond['val'],
                    'goet':  v >= cond['val'],
                    'lt'  :  v <  cond['val'],
                    'loet':  v <= cond['val'],
                    'et'  :  v == cond['val']
                }

            return 1 if operators[cond['comp']] else 0
        
        AgroRule.evaluate(self, data=data)
        df      = self.prepare(data)
        updt_df = deepcopy(df)
        
        #This is an intensive calculation, so we decided to work
        #more 'raw' data, and then convert them into a pandas column ç.ç
        #it's just a moving-applied-function (eg. moving-average)
        target_values         = updt_df[self.target].values
        applied_target_values = [apply_condition(self.aggregation_fnc(target_values[i-self.timeframe:i]), self.condition) if i>=self.timeframe else 0.0 for i in range(len(target_values))]
        #SAME CODE BUT NO COMPREHENSION - FOR DEBUG
        # applied_target_values = list()
        # for i in range(len(target_values)):
        #     print(f"{i}| {target_values[i-self.timeframe:i] if i>=self.timeframe else [] } | {sum(target_values[i-self.timeframe:i])} | {apply_condition(sum(target_values[i-self.timeframe:i]), self.condition) if i>=self.timeframe else 0.0}")
        #     applied_target_values.append(apply_condition(sum(target_values[i-self.timeframe:i-1]), self.condition) if i>=self.timeframe else 0.0)
        updt_df[self.column]  = applied_target_values

        updt_df = self.to_actual_mapping(data=updt_df)

        return self.finalize(data=updt_df)

#This apply a function on a column, breaking on certain condition.
#On break set the default value. You can specify a start_condition
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class ApplyAndBreakOnCondition(ComparativeRule):
    """
    This apply a function on a column, breaking on certain condition.
    On break set the default value. You can specify a start_condition.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        break_condition (str): Express the condition that must be met to be stopped.
        start_condition (str): Express the condition that must be met to be started.
        reset_value (int): After the break this value is set.
        fnc (Callable): Function to apply to the target column.
    """

    def __init__(self, fnc:Callable, reset_value:float, break_condition:str, **kwargs) -> None:
        super().__init__(**kwargs, condition=break_condition)
        self.fnc                = get_mapped_function(fnc)
        self.reset_value        = reset_value
        self.start_condition    = self.get_condition(kwargs.get('start_condition', None)) if kwargs.get('start_condition', None)!=None else None

    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        This method evaluates the rule on the input data.
        It applies the condition logic over the entire target column.
        
        Args:
            data (pd.DataFrame): Input dataframe to evaluate
            
        Returns:
            pd.DataFrame with rule applied
        """
        def apply_condition(v, cond:dict):
            """
            This function applies the condition logic to a single value.
            
            Args:
                v: Value to check condition against
                cond (Dict): Condition dictionary with comparison operator and value
                
            Returns:
                int: 1 if condition met, 0 otherwise
            """
            #Mapping of operators, to make the code more readable.
            operators = {
                    'gt'  :  v >  cond['val'],
                    'goet':  v >= cond['val'],
                    'lt'  :  v <  cond['val'],
                    'loet':  v <= cond['val'],
                    'et'  :  v == cond['val']
                }

            return 1 if operators[cond['comp']] else 0
        def apply_or_break(column_data:List[float], condition:dict) -> List[float]:
            """
            This function applies the condition logic to a list of values.
            If the condition is met, the list is reset with the reset_value.
            If the condition is not met, the list is appended with the next value.
            
            Args:
                column_data (List[float]): List of values to process
                condition (Dict): Condition dictionary with comparison operator and value
                
            Returns:
                List[float]: Processed values
            """
            #This is the return value
            applied_data    = list()
            progr_fnc_vals  = []
            start_flag      = False
            break_flag      = True
            for i in range(len(column_data)):
                #Checking, if specified, the start flag
                if self.start_condition != None:
                    if break_flag==True:
                        if apply_condition(v=float(column_data[i]), cond=self.start_condition)==1:
                            start_flag = True
                            break_flag = False
                else:
                    #Otherwise is always true
                    start_flag = True
                    break_flag = False
                
                #Checking start flag.
                #it is true when start_condition is None, or match the condition
                #on the current line.
                if start_flag:
                    if apply_condition(v=float(column_data[i]), cond=condition):
                        progr_fnc_vals = [self.reset_value]
                        applied_data.append(self.fnc(progr_fnc_vals))
                        break_flag = True
                        start_flag = False
                    else:
                        progr_fnc_vals.append(float(column_data[i]))
                        applied_data.append(self.fnc(progr_fnc_vals))
                else:
                    #it is false when start_condition is specified but does not match
                    #the condition on the current line.
                    progr_fnc_vals = [self.reset_value]
                    applied_data.append(self.fnc(progr_fnc_vals))
                    break_flag = True
            return applied_data
        
        AgroRule.evaluate(self, data=data)

        updt_df = deepcopy(data)

        #then we define a new column
        updt_df[self.column] = apply_or_break(column_data=updt_df[self.target].values, condition=self.condition)
        updt_df = self.to_actual_mapping(data=updt_df)
        return self.finalize(data=updt_df)
    
#this class rapresents a COMPOUND rule, 
#made up by two or more AgroRule. They are internally 
#evaluated. Calculations are NOT explicitated into dataframe.
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class AndComparativeAgroRule(ComparativeRule, ReflectiveAgroRule):
    """
    This class rapresents a COMPOUND rule. made up by two or more AgroRule. They are internally 
    evaluated. Calculations are NOT explicitated into dataframe

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        rules (List[AgroRule]): List of rules that are evaluated and concatenated as and rule.
    """

    def __init__(self, rules:List[AgroRule], **kwargs) -> None:
        super().__init__(**kwargs, rules=rules)
        self.rules            = rules
        self.reflective_rules = None
        self.fnc              = and_rule_over_row
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        This method prepares the input data for the evaluation.
        
        Args:
            data (pd.DataFrame): Input dataframe to prepare
            
        Returns:
            pd.DataFrame with target column prepared
        """
        #Auto-detect reflective rules, based on same target==column
        #target from inner rule, column from main rule.
        self.reflective_rules = list()
        for i, _ in enumerate(self.rules):
            if (str(self.rules[i].target) == str(self.column)) \
                or isinstance(self.rules[i], ReflectiveAgroFactor):
                self.reflective_rules.append(self.rules[i])
                self.rules.pop(i)
                     
        return super().prepare(data)

    #First we need to evaluate each rule in self.rules
    #Then we evaluate the rule of this class scope.
    def evaluate(self, data:pd.DataFrame) -> pd.DataFrame: 
        """
        This method evaluates the rule on the input data.
        It applies the condition logic over the entire target column.
        
        Args:
            data (pd.DataFrame): Input dataframe to evaluate
            
        Returns:
            pd.DataFrame with rule applied
        """
        super().evaluate(data=data)
        updt_df = deepcopy(data)
        updt_df = self.prepare(data=updt_df)

        #Evaluating base cases (No-Reflection)
        for r in self.rules:
            if not r.is_implicit:
                updt_df = r.evaluate(data=updt_df)
            else: 
                implicit_data = r.evaluate(data=updt_df)
                #Merge implicit_data with updt_df - that contains the 
                #effective dataframe - result.
                updt_df.reset_index(inplace=True, drop=True)
                implicit_data.reset_index(inplace=True, drop=True)
                #updt_df = pd.merge(left=updt_df, right=implicit_data, on=['sampleDate'])
                updt_df = pd.merge(left=updt_df, right=implicit_data, on='sampleDate', how='inner')

        #then we define a new colum
        #Applying all non-reflective rules
        updt_df['INTERNAL_'+self.column] = updt_df.apply(lambda row: self.fnc(row=row, columns=[r.column for r in self.rules]), axis=1)

        #Evaluating reflecting cases
        if self.is_reflective:
            #Applying reflective rules, one by one
            #they have the same target.
            updt_df = self.reflective_evaluate(
                data=updt_df, 
                fnc=self.fnc, 
                reflective_rules=self.reflective_rules,
                involved_columns=['INTERNAL_'+self.column])
            updt_df[self.column] = updt_df['reflective_'+self.column]
            updt_df.drop('reflective_'+self.column, axis=1, inplace=True)
        else:
            #If there are no reflective rule
            updt_df[self.column] = updt_df['INTERNAL_'+self.column]
        
        updt_df.drop('INTERNAL_'+self.column, axis=1, inplace=True)

        updt_df = self.to_actual_mapping(data=updt_df)
        return self.finalize(data=updt_df, involved_rules=self.rules)

#this class rapresents a COMPOUND rule, 
#made up by two or more AgroRule. They are internally 
#evaluated. Calculations are NOT explicitated into dataframe.
#ON_CONDITION FILTERING IS <NOT> ACTIVE
class OrComparativeAgroRule(ComparativeRule, ReflectiveAgroRule):
    """
    This class rapresents a COMPOUND rule. made up by two or more AgroRule. They are internally 
    evaluated. Calculations are NOT explicitated into dataframe

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
        rules (List[AgroRule]): List of rules that are evaluated and concatenated as and rule.
    """

    def __init__(self, rules:List[AgroRule],**kwargs) -> None:
        super().__init__(**kwargs, rules=rules, parent_rule=None)
        self.rules            = rules
        self.reflective_rules = None
        self.fnc              = or_rule_over_row
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        This method prepares the input data for the evaluation.
        
        Args:
            data (pd.DataFrame): Input dataframe to prepare
            
        Returns:
            pd.DataFrame with target column prepared
        """

        #Auto-detect reflective rules, based on same target==column
        #target from inner rule, column from main rule.
        self.reflective_rules = list()
        for i, _ in enumerate(self.rules):
            if str(self.rules[i].target) == str(self.column) or \
                isinstance(self.rules[i], ReflectiveSeries) or \
                    isinstance(self.rules[i], ReflectiveAgroFactor):
                self.reflective_rules.append(self.rules[i])
                self.rules.pop(i)
        return super().prepare(data)
    
    #First we need to evaluate each rule in self.rules
    #Then we evaluate the rule of this class scope.
    def evaluate(self, data:pd.DataFrame) -> pd.DataFrame: 
        """
        This method evaluates the rule on the input data.
        It applies the condition logic over the entire target column.
        
        Args:
            data (pd.DataFrame): Input dataframe to evaluate
            
        Returns:
            pd.DataFrame with rule applied
        """
       
        super().evaluate(data=data)
        updt_df = deepcopy(data)
        updt_df = self.prepare(data=updt_df)

        #Evaluating base cases (No-Reflection)
        for r in self.rules:
            if not isinstance(r, ReflectiveSeries):
                if not r.is_implicit:
                    updt_df = r.evaluate(data=updt_df)
                else: 
                    print(f"Running implicit column: {r.column} | {type(r)}")
                    implicit_data = r.evaluate(data=updt_df)
                    #Merge implicit_data with updt_df - that contains the 
                    #effective dataframe - result.
                    updt_df.reset_index(inplace=True, drop=True)
                    implicit_data.reset_index(inplace=True, drop=True)
                    print(f"implicit_data: {r.column} has {len(implicit_data)} lines")
                    print(f"updt_df      :  has {len(implicit_data)} lines")
                    #updt_df = pd.merge(left=updt_df, right=implicit_data, on=['sampleDate'])
                    updt_df = pd.merge(left=updt_df, right=implicit_data, on='sampleDate', how='inner')
        
        #then we define a new colum
        #Applying all non-reflective rules
        updt_df['INTERNAL_'+self.column] = updt_df.apply(lambda row: self.fnc(row=row, columns=[r.column for r in self.rules]), axis=1)

        #Evaluating reflecting cases
        if self.is_reflective:
            #Applying reflective rules, one by one
            #they have the same target.
            updt_df = self.reflective_evaluate(
                data=updt_df, 
                fnc=self.fnc, 
                reflective_rules=self.reflective_rules,
                involved_columns=['INTERNAL_'+self.column])
            updt_df[self.column] = updt_df['reflective_'+self.column]
            updt_df.drop('reflective_'+self.column, axis=1, inplace=True)
        else:
            #If there are no reflective rule
            updt_df[self.column] = updt_df['INTERNAL_'+self.column]
        updt_df.drop('INTERNAL_'+self.column, axis=1, inplace=True)

        updt_df = self.to_actual_mapping(data=updt_df)
        return self.finalize(data=updt_df, involved_rules=self.rules)

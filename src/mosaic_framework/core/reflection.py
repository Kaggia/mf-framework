################################################################################
# Module: reflection.py
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

from mosaic_framework.core.protocols import ProtocolReflectiveAgroRule
from mosaic_framework.core.agronomical_factors import AgroRule

class ReflectiveAgroRule(ProtocolReflectiveAgroRule):
    """
    Basic AgroRule that allows calculation of reflective rules.
    A reflective rule means that the result depends on OLD results
    of the same targeted column. It will be inherited by a compound
    rule, and will be applied after the 'column' calc logic.
    It works on Pandas.Dataframe column, line by line.
    """
    def __init__(self) -> None:
        pass
    
    def reflective_evaluate(self, data: pd.DataFrame, fnc: Callable, reflective_rules: List[AgroRule], involved_columns: List[str], involved_condition=None, reflective_condition: AgroRule=None) -> pd.DataFrame:
        """
        Evaluates reflective rules line by line on the input data.

        Parameters:
            data: Dataframe containing all required columns
            fnc: Function to be applied to each row
            reflective_rules: List of identified reflective rules
            involved_columns: List of column names involved in calculation
            involved_condition: Optional condition column name
            reflective_condition: Optional complex condition containing reflective factors

        Returns:
            pd.DataFrame: Updated dataframe with reflective calculations applied
        """

        #this method allow to convert bracket columns (eg. factor_7[-1])
        #into a standard column (eg. factor_7)
        #WARN: This may incur into an error when we are dealing with
        #en eventual multiple ReflectiveCalculations
        #(eg. factor_7[-1], factor_7[-2], ...)
        def get_unique_involved_columns(involved_columns: List[str]) -> List[str]:
            return [c.split('[')[0] if '[' in c else c for c in involved_columns]
        
        #Return longest list between :
        # default values from reflective rules
        # default values from reflective condition rules
        def get_default_values(default_reflective_values: List[float], default_reflective_condition: List[float]) -> List[float]:
            if default_reflective_values == None:
                return default_reflective_condition
            if default_reflective_condition == None:
                return default_reflective_values    
            internal_dict = {
                len(default_reflective_values): default_reflective_values,
                len(default_reflective_condition): default_reflective_condition,
            }
            return internal_dict[max(list(internal_dict.keys()))]
        
        #First we define a temporary column - WARN: With multiple reflective columns?
        updt_data = deepcopy(data)

        #check wether a reflective condition is on stage, 
        #we will use to filter reflective operations on 
        #reflective condition
        is_reflective_condition = bool(reflective_condition!=None)
        if is_reflective_condition:
            reflective_condition.prepare(data=data)
        #target is the same column name of 'resulting' column
        #containing the partial result of the compound rule
        #Using (A && B) && C where C is the reflective part
        #Using (A || B) || C where C is the reflective part
        reflective_column            = reflective_rules[0].target if len(reflective_rules)>0 else reflective_condition.reflective_rules[0].target
        #Get the complete list of columns to calculate the function
        total_columns_involved = list(set(get_unique_involved_columns(involved_columns) + [reflective_column]))

        updt_data[reflective_column] = np.nan 
        reflective_data              = updt_data[reflective_column].values

        default_values_cond = None
        if is_reflective_condition:
            updt_data[reflective_condition.column] = np.nan 
            reflective_data_cond              = updt_data[reflective_condition.column].values
            #We need to solve the 'non-reflective columns'
            #Cause will be expensive calculated each time the column
            #and get the sigle value
            non_reflective_columns_data = dict()
            for r in reflective_condition.rules:
                non_reflective_columns_data[r.column] = r.evaluate(updt_data)[r.column].values

            default_values_cond = [0.0 for _ in range(max([rc.ref if 'ref' in rc.__dict__.keys() else rc.ref_start for rc in reflective_condition.reflective_rules]))]
            # default_values_cond = \
            #     [0.0 for _ in range(max([r.ref if 'ref' in r.__dict__.keys() else r.ref_start for r in reflective_condition.reflective_rules]))] \
            #     if is_reflective_condition \
            #     else None

        #First we need to set a certain amount of values in 'updt_data[temporary_column]'
        #to 0.0, in order to let the calculations start correctly, cause with 
        #reflective rules we are point to older calculated data. So we take 
        #the max between ref values in each reflective rule.
        #We added check on len(reflective_rules)>0 because it happens that we have
        #reflection on condition and not on formula.
        default_values      = [0.0 for _ in range(max([r.ref if 'ref' in r.__dict__.keys() else r.ref_start for r in reflective_rules]))] \
            if len(reflective_rules)>0 \
            else []

        default_values      = get_default_values(default_reflective_values=default_values, default_reflective_condition=default_values_cond)
        default_values_cond = [0.0] if default_values_cond == None else default_values_cond
        for i in range(len(default_values)):
            updt_data.at[i, reflective_column] = default_values[i]
            reflective_data[i]                 = default_values[i]
            #on eventual reflective condition
            if is_reflective_condition:
                updt_data.at[i, reflective_condition.column] = default_values[i]
                reflective_data_cond[i]                 = default_values[i]
        
        #Start the reflective calculation, row by row
        #Cause each result depends on the previous values.
        if not reflective_rules and is_reflective_condition:
            reflective_rules = reflective_condition.reflective_rules
        cond_eval = -1
        for i, row in updt_data.iterrows():
            for ref_rule in reflective_rules:
                #Skip default data
                if i>=len(max(default_values, default_values_cond)):
                    if is_reflective_condition:
                        #This condition is evaluated by 'values' not by column
                        #So first we need to create row to evaluate.
                        #reflective condition has mixed conditions in it.
                        condition_row = {}
                        #First we need to get the values for each non reflective condition column
                        #already calculated
                        for non_ref_col in list(non_reflective_columns_data.keys()):
                            condition_row[non_ref_col] = non_reflective_columns_data[non_ref_col][i]
                        #then we calculate each of reflective value
                        for rr in reflective_condition.reflective_rules:
                            condition_row[rr.column] = rr.evaluate(updt_data, i)
                        
                        cond_eval = reflective_condition.fnc(row=condition_row, columns=list(condition_row.keys()))
                        updt_data.at[i, reflective_condition.column] = cond_eval
                    #Evaluate the reflective rule ONLY if there's a 
                    #TRUE reflective condition (1.0)
                    #or there's any of them (-1)
                    evaluation = \
                        ref_rule.evaluate(data=updt_data, actual_index=i) \
                            if cond_eval==-1 or cond_eval==1 \
                            else 0.0
                    updt_data.at[i, reflective_column] = evaluation
                    #adding the already calculated data (non-reflective input)    
                    #fnc is OR | AND | EVAL
                    #calculate the function
                    single_row = {k: row.to_dict()[k] for k in list(total_columns_involved + [involved_condition] if involved_condition is not None else total_columns_involved)}
                    #single_row = {k:row.to_dict()[k] for k in list(total_columns_involved)}
                    if is_reflective_condition:
                        single_row[reflective_condition.column] = cond_eval
                    single_row[reflective_column] = evaluation

                    actual_result = fnc(row=single_row, columns=total_columns_involved)
                    updt_data.at[i, reflective_column] = actual_result

                    #print("sampleDate: ", row['sampleDate'], "data: ", single_row, "result: ", actual_result)
                    #Append the result and update the reflective column
                    reflective_data[i] = actual_result
                    # if is_reflective_condition:
                    #     print(f"factor={reflective_column}", f"condition_row={condition_row}" , " | ", f"cond_eval={cond_eval}" , " | ", f"actual_result={actual_result}")
                    # else:
                    #     print(f"factor={reflective_column}", f"actual_result={actual_result}")

        updt_data['reflective_'+reflective_column] = reflective_data
        return updt_data

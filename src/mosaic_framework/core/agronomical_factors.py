################################################################################
# Module: agronomical_factors.py
# Description: Implementation of agronomical factors.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING


import re
import random
import string
import pandas as pd
from copy import deepcopy
from typing import List
from warnings import warn
import time

if TYPE_CHECKING:
    from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
    
    MosaicRulesHubType  = MosaicRulesHub

from mosaic_framework.core.protocols import ProtocolAgroRule
from mosaic_framework.core.exceptions import ColumnNameError

from mosaic_framework.core.environment.rules_hub import MosaicRulesHub

#Basic AgroRule, it helps to join all the basic methods.
class AgroRule(ProtocolAgroRule):
    """
    AgroRule is a class that represents a basic agronomical factor.
    It is initialized with a target column name, a boolean indicating

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, target, column:str, is_implicit:bool, on_condition:object=None, debug:bool=False) -> None:
        self.is_implicit = is_implicit
        self.column      = self.__get_column(col=column)
        self.target      = target
        self.on_condition= on_condition if on_condition!=None else EmptyAgroRule()
        self.has_reflective_rule = self.is_reflective_rule()
        self.has_reflective_condition = self.is_reflective_condition()
        self.is_reflective = \
            self.has_reflective_rule or self.has_reflective_condition
        self.debug  = debug
        self.start_time  = None
        self.end_time    = None
        self.rules_hub   = None
    
    #Based on what value is column and if it is implicit, 
    #returns a fixed or rnd string
    def __get_column(self, col) -> str:
        """
        Get column name.
        If is_implicit=False and col is None, raise TypeError.
        If is_implicit=True and col is not None, raise warning and ignore col.

        Args:
            col: Column name to process

        Returns:
            str: Processed column name
        """
        #If column is explicit (needed to be shown as result)
        #But is currently None, then raise TypeError
        if self.is_implicit == False and col==None:
            raise TypeError("Column must be specified, cause is_implicit=False.")
        #Raise a warning if is not explicit but a column is specified.

        elif self.is_implicit == True and col!= None:
            warn(f"WARNING: is_implicit={self.is_implicit} but column has been specified. It will be ignored.")
                
        return col if col!=None else 'column_'+''.join(random.choice(string.ascii_letters + string.digits) for _ in range(5))

    #Check if column name chosen is already present in the dataframe, 
    #otherwise return True
    def validate_column_name(self, data:pd.DataFrame) -> bool:
        """
        Validate column name.
        If the column name is already present in the DataFrame, raise a ColumnNameError.

        Args:
            data (pd.DataFrame): DataFrame to validate column name against

        Returns:
            bool: True if column name is valid
        """
        #Need to check if another column has the same name, 
        #otherwise increase the counter
        #print(f"Checking with <AgroRule>: {self.column} | {[c for c in data.columns]}")
        if sum([1 for c in data.columns if c==self.column])>1:
            raise ColumnNameError(f"{self.column} is already present in the DataFrame. Chose another one.")
        return True

    #Get a bool describing wether the rule is reflective on evaluation
    def is_reflective_rule(self)->bool:
        """
        Check if the rule is reflective.
        Returns True if the target column contains any integer enclosed in square brackets, e.g., [-10].

        Returns:
            bool: Whether rule is reflective
        """
        def contains_integer_in_brackets(target_col:str):
            return bool(re.search(r'\[-[1-9]\d*\]', target_col))
        
        is_reflective = None
        
        if self.target != None:
            if isinstance(self.target, List):
                is_reflective = any([contains_integer_in_brackets(target_col=t) for t in self.target])
            else:
                is_reflective = contains_integer_in_brackets(target_col=self.target)
        else: 
            is_reflective = False
        
        return is_reflective
    
    #Get a bool describing wether the rule is reflective on condition
    def is_reflective_condition(self)->bool:
        """
        Check if the condition is reflective.
        Returns True if the target column of the condition contains any integer enclosed in square brackets, e.g., [-10].

        Returns:
            bool: Whether condition is reflective
        """
        #Check if the condition has a reflective rule
        #in its definition
        def contains_integer_in_brackets(target_col:str):
            return bool(re.search(r'\[-[1-9]\d*\]', target_col))
        def is_complex_condition(on_condition):
            return 'rules' in list(on_condition.__dict__.keys())
        
        is_reflective = None
        if self.on_condition != None and self.on_condition != "None":
            #Check wether if we have a complex (AND | OR | etc.) or simple condition
            if is_complex_condition(self.on_condition):
                #this means that we need to check if we have any reflection in 
                #rules, that is a list of Conditions. 
                #[WARNING] Remember that we can check at 1-level, it's not a 
                #          recursive check.
                #Check is different, cause reflection in conditions can be checked by a
                #corrispondence  1 on 1 between 'column' and 'target' of on_condition
                #rules.
                is_reflective = any([r.target==self.column for r in self.on_condition.rules])
            else:
                #this means that we need to check if we have any reflection in 
                #a single variable that is target.
                if self.on_condition.target != None and self.on_condition.target != "None":
                    is_reflective = contains_integer_in_brackets(target_col=self.on_condition.target)
                else: 
                    is_reflective = False
        else: 
            is_reflective = False
        
        return is_reflective
    
    #prepare dataframe indexing sample_date
    def prepare(self, data:pd.DataFrame)  -> pd.DataFrame:
        """
        Prepare the data for evaluation.

        Args:
            data (pd.DataFrame): DataFrame to prepare

        Returns:
            pd.DataFrame: Prepared DataFrame
        """
        self.debug = self.debug or self.rules_hub.get_variable("debug").content
        self.rules_hub.register(rule=self)
        return data
    
    #returns None
    def evaluate(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the rule on the given data.

        Args:
            data (pd.DataFrame): DataFrame to evaluate

        Returns:
            pd.DataFrame: Evaluated DataFrame
        """

        data = self.prepare(data=data)

        if isinstance(self.on_condition, EmptyAgroRule) or isinstance(self.on_condition, AgroRule):
            self.on_condition.set_rules_hub(rules_hub=self.rules_hub)
                
        if self.debug: print(f"Evaluating  : {self.column + ' '*(25-len(self.column))} | {str(type(self))[str(type(self)).rfind('.')+1:-2]}")
        
        self.start_time = time.time()
        updt_df         = deepcopy(data)

        #We want to pre-calculate the 'on_condition', 
        #based on the fact weather it is, or not, a 
        #reflective condition, or in a reflective rule
        if not self.has_reflective_condition and self.on_condition != "None":
            if self.debug: print(f"|  └   '{self.column}' is not a reflective rule. Calculating the 'on_condition'.")
            on_cond_result      = self.on_condition.evaluate(data=data)
            on_cond_result_data = on_cond_result[self.on_condition.column].values
            updt_df[self.on_condition.column] = on_cond_result_data
        elif self.on_condition == "None":
            if self.debug: print(f"|  └   '{self.column}' is <EmptyAgroRule> column. Skipping on_condition pre-calculation.")
        else:
            if self.debug: print(f"|  └   '{self.column}' is REFLECTIVE rule. Skipping on_condition pre-calculation.")
        return updt_df

    #last piece of evaluating, dropping a column if is_implicit == True
    #Also we need to return the calculated column, with the index, 
    #in order to allow merge resulting column
    def finalize(self, data:pd.DataFrame, involved_rules:List[ProtocolAgroRule]=None, debug:bool=False) -> pd.DataFrame:
        """
        Last part of evaluation of the rule. 
        Dropping the column if is_implicit == True.
        Also apply condition if rule is not reflective.

        Args:
            data (pd.DataFrame): DataFrame to finalize
            involved_rules (List[ProtocolAgroRule], optional): List of involved rules. Defaults to None.
            debug (bool, optional): Whether to print debug info. Defaults to False.

        Returns:
            pd.DataFrame: Finalized DataFrame
        """
        def remove_implicit_columns(final_df:pd.DataFrame, involved_rules:List[ProtocolAgroRule]):
            #diff_columns are colums already deleted
            #removing all columns found in rules <ProtocolAgroRule> objects.
            #also rules can be a single rule
            keep_columns = final_df.columns.to_list()
            if involved_rules != None:
                for r in involved_rules if isinstance(involved_rules, List) else [involved_rules]:
                    if r.is_implicit and r.column in final_df.columns:
                        keep_columns.remove(r.column)
            return keep_columns
        
        final_df = deepcopy(data)

        output_column = self.column if self.validate_column_name(data=data) else None
        
        # #Filtering result, based on condition, if it is not reflective, 
        # #it allows to have a fallback whenever reflection is not applied,
        # #and we need to filter result for a certain condition.
        if not self.is_reflective: 
            if pd.api.types.is_numeric_dtype(final_df[self.column]):
                final_df[self.column] = final_df[self.column] * final_df[self.on_condition.column] if self.on_condition != "None" else final_df[self.column]
            else:
                final_df[self.column] = final_df.apply(lambda row: row[self.column] if row[self.on_condition.column] == 1 else None, axis=1)

        #Lastly we drop the column on_condition if it's a rule 
        #and it's implicit.
        if self.on_condition != "None":
            if self.on_condition.is_implicit:
                final_df.drop(self.on_condition.column, axis=1, inplace=True)
                if self.debug: print(f"|  └   on_condition_column={self.on_condition.column} dropped")

        #It is needed cause target the object itself
        if self.is_implicit:
            final_df = final_df[['sampleDate', output_column]]
        
        #removing all columns found in rules <ProtocolAgroRule> objects.
        #Also rules can be a single rule, needed for first level nesting
        keep_columns = remove_implicit_columns(final_df, involved_rules)
        final_df = final_df[keep_columns]

        #final_df.reset_index(inplace=True)
        self.end_time = time.time()
        elab_time     = f"duration: {round(self.end_time-self.start_time, 4)} seconds."
        if self.debug: print(f"└Finished   : {self.column + ' '*(25-len(self.column))} | {elab_time} | {str(type(self))[str(type(self)).rfind('.')+1:-2]}\n")
        return final_df

    def set_rules_hub(self, rules_hub:MosaicRulesHubType) -> None:
        """
        Setting the current MosaicRulesHub for the AgroRule

        Args:
            rules_hub (MosaicRulesHub): MosaicRulesHub for the current session
        """
        self.rules_hub = rules_hub

    #@override print function
    def __str__(self):
        return f'{self.__class__}: | params: {[(k, str(v))  for k, v in (self.__dict__.items())]}'

#This class has the role to define a basic behaviour, setting
#an entire column to value of 1.0, in order to be used as bypass value
#on a on_condition parameter for each one of rules.
class EmptyAgroRule(AgroRule):
    """
    EmptyAgroRule is a class that represents a basic agronomical factor.
    
    Args:
        target (str): None
        column (str): None
        is_implicit (bool): True
        on_condition (object, optional): 'None'
        debug (bool, optional): False
    """
    def __init__(self) -> None:
        super().__init__(target=None, column=None, is_implicit=True, on_condition="None", debug=False)
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for evaluation.

        Args:
            data (pd.DataFrame): DataFrame to prepare

        Returns:
            pd.DataFrame: Prepared DataFrame
        """
        return super().prepare(data)
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the rule on the given data.

        Args:
            data (pd.DataFrame): DataFrame to evaluate

        Returns:
            pd.DataFrame: Evaluated DataFrame
        """
        super().evaluate(data=data)
        data[self.column] = 1.0

        return self.finalize(data=data)

    def set_rules_hub(self, rules_hub:MosaicRulesHubType) -> None:
        """
        Setting the current MosaicRulesHub for the AgroRule

        Args:
            rules_hub (MosaicRulesHub): MosaicRulesHub for the current session
        """
        self.rules_hub = rules_hub

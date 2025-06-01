################################################################################
# Module: output_models.py
# Description: Collection of Agronomic rules, and their evaluation.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from __future__ import annotations
from typing import Any, Protocol, TYPE_CHECKING, List, Dict, Tuple

import dateutil
import pandas as pd
from copy import deepcopy
from warnings import warn
from datetime import datetime, timedelta

if TYPE_CHECKING:
    from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
    
    MosaicRulesHubType  = MosaicRulesHub

from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.output_factors import OutputAgroRule
from mosaic_framework.core.exceptions import InvalidOutputAgroRule
from mosaic_framework.dt.datetime_parser import DatetimeParser

class ProtocolOutputModel(Protocol):    
    def prepare(self) -> pd.DataFrame:
        ...

    def evaluate(self) -> object: 
        ...


class OutputModel(ProtocolOutputModel):
    """
    A model for generating output predictions based on agronomic rules.

    Parameters:
        label (str): Label identifier for this model instance
        previsionDay (str): Date for which prediction is made
        days (int): Number of days to predict
        data (pd.DataFrame): Input data for predictions
        history (Tuple): Historical data window parameters
        rules_hub (MosaicRulesHubType): Rules management hub
        output_rule (OutputAgroRule, optional): Rule for generating output. Defaults to None
        risk_window (Tuple[int, int, int]): Window parameters for risk calculation. Defaults to (2,1,2)
        prevision_window (Tuple[int, int, int]): Window parameters for prediction. Defaults to (0,1,5)
    """
    def __init__(self, label:str, previsionDay:str, days:int, data:pd.DataFrame, history:Tuple, rules_hub:MosaicRulesHubType, output_rule=None, risk_window:Tuple[int,int,int]=(2, 1, 2), prevision_window:Tuple[int,int,int]=(0, 1, 5)) -> None:
        self.data             = data
        self.label            = label
        self.previsionDay     = previsionDay
        self.days             = days
        self.history          = history             #needed to estimate over the entire series 
        self.rules            = list()
        self.output_rule      = output_rule
        self.rules_hub        = rules_hub

    def get_window(self) -> Tuple[int, int]:
        """
        Gets a unified window combining history, risk and prediction periods.
        
        Returns:
            Tuple[int, int]: Start and end offsets for the window
        """
        return (
            self.history[0], 
            self.days+1)
    
    def extract_data(self, data:pd.DataFrame, start:datetime, end:datetime) -> pd.DataFrame:
        """
        Extracts data between start and end dates.

        Parameters:
            data (pd.DataFrame): Source dataframe
            start (datetime): Start date
            end (datetime): End date

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        extr_data = deepcopy(data)
        return extr_data
    
    def add_factor(self, factor:AgroRule) -> None:
        """
        Adds an agronomic rule factor to the model.

        Parameters:
            factor (AgroRule): Rule to add
        """
        self.rules.append(factor)
        print(f"[OutputModel] Factor: <{type(factor)}>: {factor.column}  appended.")

    def set_output_rule(self, factor:Any) -> None:
        """
        Sets the output rule, converting to list format if needed.

        Parameters:
            factor (Any): Rule to set as output
        """
        if self.output_rule == None:
            self.output_rule = factor
        
        if not isinstance(self.output_rule, List):
            self.output_rule = [self.output_rule]
        return 

    def is_valid_output_rule(self) -> bool:
        """
        Validates that output rules are properly formatted.

        Returns:
            bool: True if rules are valid

        Raises:
            InvalidOutputAgroRule: If rules are invalid
        """
        if not isinstance(self.output_rule, List):
            raise InvalidOutputAgroRule(f"OutputModel allows only List[OutputAgroRule] to be calculated, found :{type(self.output_rule)}.")
        
        for out_r in self.output_rule:
            if not isinstance(out_r, OutputAgroRule):
                raise InvalidOutputAgroRule(f"OutputModel allows only OutputAgroRule to be calculated, found :{type(out_r)}.")

        return True
    
    def prepare(self) -> pd.DataFrame:
        """
        Prepares input data by filtering to required date range.

        Returns:
            pd.DataFrame: Prepared dataframe
        """
        dt_parser       = DatetimeParser()
        prep_data       = deepcopy(self.data)
        prep_data['dt'] = pd.to_datetime(dt_parser.parse_batch(prep_data['sampleDate'].to_list()))
        calculation_window = self.get_window()
        print(f"[OutputModel] Calculation window is: {calculation_window}")
        start = pd.to_datetime(self.previsionDay - timedelta(days=calculation_window[0]))
        end   = pd.to_datetime(self.previsionDay + timedelta(days=calculation_window[1]))

        print(f"[OutputModel] Calculating from: {start} to {end}")
        prep_data = prep_data[\
            (prep_data['dt']>=start)&\
            (prep_data['dt']<end)]
        prep_data.drop('dt', axis=1, inplace=True)
        prep_data['sampleDate'] = prep_data['sampleDate'].apply(lambda x:dateutil.parser.parse(x).strftime("%Y-%m-%d %H:%M"))
        prep_data.reset_index(inplace=True, drop=True)
        return prep_data
    
    def validation(self) -> bool:
        """
        Validates input data and rules.

        Returns:
            bool: True if validation passes
        """
        self.is_valid_output_rule()        
        return True
    
    def estimate(self) -> Tuple[pd.DataFrame, pd.DataFrame]: 
        """
        Estimates output risk by applying all rules.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Tuple containing hourly and daily results
        """
        data = self.prepare()
        calculation_window = self.get_window()
        self.validation()
        
        print(f"[OutputModel] Estimating: {self.label}")
        try:
            data.to_csv(f"results/{self.label}_start_dataset.csv")
        except:
            print(f"CANNOT PRINT {self.label}_start_dataset.csv")

        for r in self.rules:
            data = r.evaluate(data) 
            try:
                data.to_csv(f"results/{self.label}_rules.csv")
            except:
                print(f"CANNOT PRINT {self.label}_rules.csv")
        
        results = compact_results = None
        results = deepcopy(data)
        for output_rule in self.output_rule:
            compact_results, results = output_rule.evaluate(data=results)
        
        results         = self.rules_hub.remove_implicit_columns(data=results)
        compact_results = self.rules_hub.remove_implicit_columns(data=compact_results)

        try:
            results.to_csv(f"results/{self.label}_rules.csv")
        except:
            print(f"CANNOT PRINT {self.label}_rules.csv")

        try:
            compact_results.to_csv(f"results/{self.label}_result.csv")
        except:
            print(f"CANNOT PRINT {self.label}_result.csv")

        return results, compact_results
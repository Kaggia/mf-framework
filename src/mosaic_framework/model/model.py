################################################################################
# Module:      model.py
# Description: Handle the model, Calculate all rules in it.
# Author:      Stefano Zimmitti
# Date:        27/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

from copy import deepcopy
import pandas as pd
from typing import Any
import pkgutil
import importlib
import dateutil
import json
from datetime import datetime, timedelta

import mosaic_framework.core
from mosaic_framework.config.configuration import MODEL
from mosaic_framework.data_storage.converters import Converter
from mosaic_framework.components.components import Component
from mosaic_framework.engine.rule_parser import RuleParser
from mosaic_framework.engine.output_rule_parser import OutputRuleParser
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.dt.datetime_parser import DatetimeParser
from mosaic_framework.model.exceptions import DataFormatException, RulesFormatError
from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
from mosaic_framework.core.output_model import OutputModel
from mosaic_framework.core.output_factors import OutputAgroRule

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.core.agronomical_factors import AgroRule
    

    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory
    AgroRuleType           = AgroRule

class Model(Component):
    """
    This class allow to effectively elaborate data, evaluating a set of rules, connected to an
    output. Also allow to define one or more outputs. Produces an output, by rule and by output. 

    ---\n
    Available params:
    label (str): used to connect the model with a specific input data and validation pipeline.
    output (List[str]): used to define the outputs that are needed to be calculated. 
    Also allows to validate the rules params, for each output listed a <output> param is check
    for the rules, relative to the output.
    ---\n
    Examples:\n
    label='antrapi', output=['primary', 'intermidiate', 'secondary'], primary=[...], intermidiate=[...], secondary=[...]
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=MODEL, **kwargs)
        self.tag                 = 'model'
        self.data_storage        : MosaicDataStorageType   = None
        self.shared_memory       : MosaicSharedMemoryType  = None
        self.rules_hub           = MosaicRulesHub(config=MODEL.get('data').get('rules_hub'))
        self.rule_parser         = RuleParser(rules_hub=self.rules_hub)
        self.output_rule_parser  = OutputRuleParser(rules_hub=self.rules_hub)
        self.data                = None
        self.outputs_rules       = dict()

    @staticmethod
    def append_results(actual:pd.DataFrame, to_merge:pd.DataFrame):
        """
        @staticmethod 
        Used to merge pd.DataFrame internally.
        ----\n
        params:
        actual:pd.DataFrame, actual dataframe
        to_merge:pd.DataFrame, dataframe that is needed to be appended.
        """
        return pd.concat([actual, to_merge]) if not actual.empty else to_merge

    def get_modules(self)->list:
        """
        Run over mosaic_framework package, find any module that is part of the package
        then add them to a list. If self.DEBUG=True then displays the found modules. 
        Modules are found dynamically at runtime.
        ---\n
        params:
        None
        ---
        returns: List of modules find in mosaic_framework path.
        """
        # Scorri tutti i moduli nel package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.core.__path__, mosaic_framework.core.__name__ + '.'):
            # Importa il modulo dinamicamente
            module = importlib.import_module(module_name)
            # Aggiungi il modulo alla lista
            modules.append(module)    
        return modules

    def get_debug(self)->bool:
        return True if self.debug=='active' else False
    
    def get_v_look_up_table(self)->dict:
        """
        This function is used to handle the v_look_up_table, in particular it is used to 
        handle the growth_model, that is a list of dicts, each dict is a growth model.
        """
        v_look_up_table = self.shared_memory.get_variable('v_look_up_table')
        if v_look_up_table is None:
            self.shared_memory.add_variable(key='v_look_up_table', content={}, is_immutable=False)
            v_look_up_table = self.shared_memory.get_variable('v_look_up_table')
        return v_look_up_table.content
    
    def prepare(self)->None:
        """
        Prepare the Model to elaborate, in particular we analyze the unparsed output rule, 
        contained in output_labels, that may vary between Models.
        ---\n
        params:
        None
        ---\n
        returns: Boolean value (True) wether or not checks are passed, otherwise 
        error is raised.
        """

        super().prepare()

        #Parsing bottom-to-top the rules find in each output param.
        for output_label in self.outputs:
            unparsed_rules = self.__dict__.get(output_label)
            print(f"[Model] Parsing outputs rules :{output_label}")
            # parsed_rules   = self.get_parsed_rules(
            #     output_rules=unparsed_rules, 
            #     core_rules_modules=self.get_modules())
            parsed_rules   = self.rule_parser.parse(output_rules=unparsed_rules)
            self.outputs_rules[output_label] = parsed_rules
        
        
        #Parsing OutputRules.
        for output_label in self.outputs:
            #forcing to a list if it is not
            if not isinstance(self.__dict__[output_label+'_output_rule'], list):
                self.__dict__[output_label+'_output_rule'] = [self.__dict__[output_label+'_output_rule']]
            #parsing one by one.
            self.__dict__[output_label+'_output_rule'] = \
                [self.output_rule_parser.parse(output_rule=outr) for outr in self.__dict__[output_label+'_output_rule']]
        return
    
    def get_data(self)->pd.DataFrame:
        """
        In order to process data we need to got them from the available connectors, where
        data references reside. First connector with the same connect_out of Model label 
        is retrieved, then the Resource is got from it and furtherly processed if it is not
        a Pandas DataFrame.
        ---\n
        params:
        None
        ---\n
        returns: Boolean value (True) wether or not checks are passed, otherwise 
        error is raised.
        """
        connectors               = self.shared_memory.get_variable('connectors').content
        resource : Resource  = None
        #looking for connect_out == self.label
        for c in connectors:
            if c['connect_out'] == self.label:
                resource = c['resource']
                break
        raw_data  = resource.get_data()
        converter = Converter()
        data      = converter.to_data_format(data=raw_data, data_format='dataframe')
        return data
    
    def validate_outputs(self)->bool:
        """
        Validate each parameter relative to output found in the component definition, 
        checks are implemeted as guards. If checks are fulfilled True is returned. 
        Also here rules presence is checked, for each output found in outputs.
        ---\n
        params:
        None
        ---\n
        returns: Boolean value (True) wether or not checks are passed, otherwise 
        error is raised.
        """

        #Check if rules are in place
        for output_rules_name in self.outputs:
            if len(self.__dict__.get(output_rules_name, None)) == 0:
                raise RulesFormatError(f"{output_rules_name} does not have rules in it.")
        
        #Check for each output if <OUTPUT_NAME>_output_rule is present
        #Check if type is correct.
        for output_label in self.outputs:
            if not output_label+'_output_rule' in list(self.__dict__.keys()):
                RulesFormatError(f"{output_label+'_output_rule'} is not in Model definition.")
            if not isinstance(self.__dict__[output_label+'_output_rule'], OutputAgroRule):
                RulesFormatError(f"{output_label+'_output_rule'} is not in a valid OutputAgroRule. Found: {type(self.__dict__[output_label+'_output_rule'])}")
        
        print("[Model] Validating outputs complete")
        return True
    
    def validate_data(self, data:pd.DataFrame)->bool:
        """
        Validate each parameter relative to output found in the component definition, 
        checks are implemeted as guards. If checks are fulfilled True is returned. 
        Also here rules presence is checked, for each output found in outputs.
        ---\n
        params:
        None
        ---\n
        returns: Boolean value (True) wether or not checks are passed, otherwise 
        error is raised.
        """
        if isinstance(data, pd.DataFrame) == False:
            raise DataFormatException("Input data is not a Pandas DataFrame.")
        if len(data) == 0:
            raise DataFormatException("Input data is empty.")
        print("[Model] Validating data complete")
        return True
    
    def validate(self):
        """
        Validate wrapper method, used to run each one of the valiation operations
        .
        ---\n
        params:
        None
        ---\n
        returns: Boolean value (True) wether or not checks are passed, otherwise 
        error is raised.
        """
        #validating just the output rules presence
        self.validate_outputs()

        #validate the data needed to elaborate
        self.validate_data(data=self.data)

        return True

    def get_default_date_column(self, data:pd.DataFrame):
        """
        Get the default for date_column param, there is a set of column's name 
        available: ["sampledate", "sample_date", "date", "datetime", "time"], that 
        can be found in configurations. If column cannot be found in data's header
        than an error is raised, otherwise a good column name is returned.
        .
        ---\n
        params:
        data:pd.DataFrame
        ---\n
        returns: 
        - str: column found
        .
        """

        possible_date_columns = self.config['data']['date_column']
        cols_found = 0
        cols = list()
        for i, c in enumerate([c.lower() for c in data.columns.to_list()]):
            if c in possible_date_columns:
                cols_found +=1
                cols.append(data.columns.to_list()[i])
        if cols_found>1:
            raise DataFormatException(f"{[cf for cf in cols]} found as standard date columns. Choose one of them, and set in the Model Component as 'date_column'.")
        return cols[0]
    
    def get_default_prevision_day(self, past_days:int, data:pd.DataFrame, column:str)->datetime:
        """
        Get the default for prevision_day param. This is the starting day of the estimation
        Calculated concordingly with history param and the end of the dataset.
        .
        ---\n
        params:
        - past_days (int): days that cannot be calculated due to dependancies.
        - data (pd.DataFrame): dataset
        - column (str): date column to be used. 
        ---\n
        returns: 
        - str: column found
        .
        """
        prep_data       = deepcopy(data)
        prep_data['dt'] = pd.to_datetime(prep_data[column])
        
        default_prevision_day = min(prep_data['dt']) + timedelta(days=past_days) 
        prep_data.drop('dt', axis=1, inplace=True)
        return default_prevision_day
    
    def get_default_days(self, previsionDay:datetime, data:pd.DataFrame, future_days:int, column:str):
        """
        Get the default for days param. This param allows to understand how many days
        will be calculated by the model. Calculated concordingly with history param and the end of the dataset.
        .
        ---\n
        params:
        - previsionDay (str): Starting day of calculation.
        - data (pd.DataFrame): dataset
        - future_days (int): Days that cannot be estimated, due to prediction days needed.
        - column (str): date column to be used. 
        ---\n
        returns: 
        - str: column found
        .
        """
        prep_data       = deepcopy(data)
        prep_data['dt'] = pd.to_datetime(prep_data[column])
        
        diff = int((max(prep_data['dt']) - previsionDay).days) - future_days
        prep_data.drop('dt', axis=1, inplace=True)

        return diff
    
    def run(self)->None:
        """
        Entry point behaviour of the class. Based on the 'params', elaborate all the rules
        specified, and calculate outputs.
        ---\n
        params:
        None
        ---\n
        Returns:
        - None
        """
        super().run()
        self.prepare()

        #Setting vars for MosaicRulesHub
        self.rules_hub.add_variable("debug", content=self.get_debug(), is_immutable=True)
        self.rules_hub.add_variable("granularity", content=self.granularity, is_immutable=True)
        self.rules_hub.add_variable("v_look_up_table", content=self.get_v_look_up_table(), is_immutable=True)

        print(f"[Model] debug parameter is set: {self.rules_hub.get_variable('debug').content}")
        
        #Get data from connector (SharedMemory variable)
        self.data = self.get_data()

        #launches validate_outputs & validate_data
        self.validate()

        #Need to calculate previsionDay on demand, based on 'history' and 'risk_window'
        history         = (self.history, 0, 0)
        date_column     = self.get_default_date_column(self.data) if self.date_column=='default' else self.date_column
        previsionDay    = self.get_default_prevision_day(
            past_days=self.history, 
            data=self.data, 
            column=date_column)
        days            = \
            self.get_default_days(data=self.data, previsionDay=previsionDay, future_days=0, column=date_column) \
                if self.days == 'default'             \
                else self.days
        print(f"[Model] Initial params: history={history} | date_column={date_column} | previsionDay={previsionDay} | days={days}\n\n")
        #Istanciate and run each one of the outputs
        final_results         = pd.DataFrame(data=None)
        final_compact_results = pd.DataFrame(data=None)

        #data
        data = deepcopy(self.data)

        #Getting a standardized ISO8601 previsionDay
        dt_parser    = DatetimeParser()
        previsionDay = dt_parser.get_standard_datetime(dt_parser.parse_single(previsionDay.isoformat()))
        print(f"[Model] previsionDay: {previsionDay} | type: {type(previsionDay)}")
        for output_label in self.outputs:
            output_model = OutputModel(
                label=output_label, 
                previsionDay=previsionDay,
                data=data,
                history=history, 
                days=days, 
                rules_hub=self.rules_hub)
            #Appending all available rules for the selected output
            for r in self.__dict__.get(output_label, None):
                output_model.add_factor(factor=r)

            #Set the current output rule(s) for the selected output
            output_model.set_output_rule(factor=self.__dict__.get(output_label+'_output_rule', None))
            
            #just the results, results with columns = output_model.estimate()
            results, compact_results = output_model.estimate()
            
            #updating data with the latest OutputModel
            data = results

            final_results         = self.append_results(final_results, results)
            final_compact_results = self.append_results(final_compact_results, compact_results)
        
        #There's a BUG here, days are duplicated with 'secondary' (if inplace) 
        # that is NaN, need to understand better the case and why it does happen.
        #To understand, remove following lines, and test the case.
        final_results.dropna(inplace=True)
        final_compact_results.dropna(inplace=True)

        #Forcing compact results to have just the date-like and output columns.
        columns_filter        = [date_column]+[v for v in self.outputs]
        final_compact_results = final_compact_results[columns_filter]

        #Load self.outputs into the SharedMemory in order to be furtherly used
        self.shared_memory.add_variable(key="outputs_labels", content=self.outputs, is_immutable=True)

        #Eventually we are going to replace eventual VLookUpTable with the current one.
        self.shared_memory.update_variable(key='v_look_up_table', new_content=self.rules_hub.get_variable('v_look_up_table'))

        #load results into MosaicDataStorage
        self.data_storage.add_resource(resource=Resource(
            label=f"{self.label}_results", 
            data=final_results, 
            file_type='csv',
            description='hourly results of model'
        ))
        self.data_storage.add_resource(resource=Resource(
            label=f"{self.label}_compact_results", 
            data=final_compact_results, 
            file_type='csv',
            description='daily results of model'
        ))
        #update connectors, find the connector that has 'connect_in' equal to the
        #label of current model. Then update the Resource in it (that current value,
        #should be empty (None)) to a new dict that points to the label's resource
        #already defined in the MosaicDataStorage, so when the validator look in the 
        #SharedMemory for the resource can retrieve both of Resource (results) that 
        #are pointed by the 'resource' content

        connectors = self.shared_memory.get_variable(key='connectors').content
        for i, _ in enumerate(connectors):
            if connectors[i]['connect_in'] == self.label:
                connectors[i]['resource'] = {
                    'results_pointer'         : f"{self.label}_results",
                    'compact_results_pointer' : f"{self.label}_compact_results"
                }

        #0. Merge the results into a solo dataframe that has all columns.  
        #1. Load the results into the MosaicDataStorage, cause the connectors get the data from it.
        #2. Update the connector that have connect_in == self.label, cause it will be retrieved
        #   from the Validator Component.
        print("[Model] Closed running.")
        return
    
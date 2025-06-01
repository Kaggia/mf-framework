################################################################################
# Module:      source.py
# Description: Handle the source, load data into MosaicDataStorage.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import os
import pandas as pd
from typing import Any
import warnings
import re
import inspect

import mosaic_framework.data_storage.readers
import mosaic_framework.data_storage.writers
import mosaic_framework.environment.columns.columns
from mosaic_framework.config.configuration import SOURCE
from mosaic_framework.data_storage.resource import Resource
from mosaic_framework.data_storage.converters import Converter
from mosaic_framework.dt.datetime_parser import DatetimeParser
from mosaic_framework.components.components import Component
from mosaic_framework.engine.module_parser import ModuleParser
from mosaic_framework.validation.input_validation import InputDataValidator
from mosaic_framework.validation.input_data_filler import InputDataFiller
from mosaic_framework.environment.columns.detect_engine import LevenshteinDistanceColumnDetectEngine
from mosaic_framework.environment.exceptions import (SourceNotRecognizedException,
    ColumnsParamNotValidException)

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    from mosaic_framework.environment.columns.columns import Column 
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

class Source(Component):
    """
    This class estabilish all properties of an environment. Where to 
    get the data from, who is the analyst the actually is building, 
    evaluating the model(s). Cases supported:\n
    - environment=local & file=''           : Look for a file called like the module parsed.
    - environment=local & file='data_1.csv' : Look for a file called 'data_1.csv'
    - environment=cloud                     : Elaboration from AWS Lambda, input from 'event'
    ---\n
    Available params:
    - label(str):         Define the name of the source, useful when you want
                          to refer to a particulare source, during the elaboration flow. 
    If left empty a random string is created.
    - environment(str):   It refers to the current environment, if it is
                          cloud or local, in order to correctly retrieve the data (ex.
                          local file or event (aws lambda) or API like MeteoBlue)
    - file(str):          If environment is local, a file with that name will searched
                          in local data folder, to load the data. Will be ignored if 
                          environment is 'cloud'.
    - tag(str):           tag the processor where this component will be assigned.
    ---\n
    Examples:\n
    Source(environment=local & file='data_1.csv')\n
    Source(environment=local)\n
    Source(environment=cloud)
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=SOURCE, **kwargs)
        self.tag          = 'preprocess'
        self.data_storage : MosaicDataStorageType  = None
        self.shared_memory: MosaicSharedMemoryType = None

    @staticmethod
    def get_reader_class(module, cls_string):
        # Inspects all classes defined in the module
        for name, cls in inspect.getmembers(module, inspect.isclass):
            # Gets the source code of the class
            source = inspect.getsource(cls)
            # Searches for the string in the source code of the class
            if re.search(cls_string, source):
                return cls

    def set_structure_references(self, environment:str, **kwargs):
        """
        In order to grant a more clean environment, and always setup, we create, each
        time we run the engine: data, models, results and validation folders.
        ---\n
        params:
        - environment (str): Define the current environment (local or cloud)
        ---\n
        returns: 
        local structure (dict): Contains the structure (local | cloud) already defined.
        """
        local_structure = None
        cloud_prefix    = kwargs.get('cloud_tmp_fld', '')
        
        if environment == 'local':
            local_structure = {
                'data'       : os.getcwd() + '/' + 'data', 
                'models'     : os.getcwd() + '/' + 'models',
                'results'    : os.getcwd() + '/' + 'results',
                'validation' : os.getcwd() + '/' + 'validation'
            }
            for key, value in local_structure.items():
                if not os.path.exists(value):
                    os.mkdir(value)
                    print(f"Local folder: {value} created.")

        elif environment == 'cloud':
            local_structure = {
                'data'       : cloud_prefix + '/' + 'data', 
                'models'     : cloud_prefix + '/' + 'models',
                'results'    : cloud_prefix + '/' + 'results',
                'validation' : cloud_prefix + '/' + 'validation'
            }
            for key, value in local_structure.items():
                if not os.path.exists(value):
                    os.mkdir(value)
                    print(f"(Cloud) Local folder: {value} created.")
        else:
            raise NotImplementedError("set_local_structure is not implemented for any other value that differs from 'local'.")

        return local_structure
    
    def get_assigned_columns(self, columns:List[Column])->dict:
        """
        This method is used to getting the mapping provided by the user,
        validate and eventually return it.
        ---\n
        params:
        columns (list): List of columns to be mapped.
        ---\n
        returns:
        mapping (dict): Mapped columns.
        """
        mapping = {c.name:c.__class__.__name__ for c in columns}
        #We get here a list of object like:
        #[
        #   SampleDate(name="sample_date"),
        #   ...
        #]

        return mapping
    
    def get_columns(self, engine, detect_type:str, **kwargs)->dict:
        """
        This method is used to automatically retrieve the columns from an input file. There 
        are available two options:\n
        - auto      : no 'columns' parameter has been specified.
        - specified : 'column' are specified, we simply forward the choices.\n
        ---\n
        params:
        engine: 
        ---\n
        returns:
        mapping (dict): Mapped columns.
        """
        columns     = kwargs.get('columns', None)
        if detect_type == 'auto':
            module_parser = ModuleParser()
            classes       = module_parser.get(module=mosaic_framework.environment.columns.columns)
            mapping       = engine.run(classes=classes, data_columns=columns)
        elif detect_type == 'specified':
            mapping       = self.get_assigned_columns(columns=columns)
        else:
            raise ColumnsParamNotValidException("detect_type not implemented or wrong.")
        return mapping

    def run(self)->None:
        """
        Entry point behaviour of the class. Based on the 'params', create a new Resource
        in MosaicDataStorage location, with name given by 'label', got from 'environment'.
        ---\n
        params:
        None
        ---\n
        returns: 
        None
        """
        super().run()
        
        #Setting local structure of folder (local machine)
        try:
            self.set_structure_references(environment=self.environment)
        except OSError as ose:
            print("[Source] [Warning] Cannot set local environment folders.") 
        #set environment as variable into the SharedMemory
        environment_variable = self.shared_memory.get_variable(key='environment')
        if environment_variable != None:
            self.shared_memory.add_variable(key='environment', content=self.environment, is_immutable=True)
        else:
            print("[Source]: Environment variable in MosaicSharedMemory has already been set.")
        
        #Simply adding cloud_temp_folder to the prefix
        environment_prefix = None
        if self.environment == 'local':
            environment_prefix = ''
        elif self.environment == 'cloud':
            environment_prefix = self.shared_memory.get_variable(key='cloud_tmp_fld', error_policy='raise').content
        else:
            raise SourceNotRecognizedException(f"Cannot recognize the selected source. Selected: {self.environment}")

        chosen_ext = None
        if self.file == '' or self.file == None:
            #This is the case where an automatic choice is made,
            #taking a file called with the same same of the model 
            #that is pointed, extension is fixed with this order
            #[csv | json].
            self.file = self.data_storage\
                .get_resource('input_file_name')\
                .get_data()

            for ext in ['csv', 'json']:
                if os.path.exists(f"{str(os.getcwd())}/data/{self.file}.{ext}"):
                    chosen_ext = ext
                    break
                else:
                    print(f"Cannot find: {str(os.getcwd())}/data/{self.file}.{ext}")
            if chosen_ext == None:
                FileNotFoundError(f"Cannot correctly retrieve from local folders a file with label: {self.file}")
        
        file : str = self.file

        #If extension is not present (not calculated) then we work on self.file
        if chosen_ext == None:
            chosen_ext = file[file.rfind('.')+1:]
            file       = file.replace('.'+chosen_ext, '')

        if self.environment == 'local':
            source_filepath = f"{str(os.getcwd())}/data/{file}.{chosen_ext}"
        elif self.environment == 'cloud':
            source_filepath = f"{environment_prefix}/data/{file}.{chosen_ext}"
        else:
            raise SourceNotRecognizedException(f"Cannot recognize the selected source. Selected: {self.environment}")

        print(f"[Source]: source_filepath={source_filepath}.")

        with open(source_filepath, 'rb+') as f:
            #Flush the <input_file_name>.[csv|json|xlsx] into DataStorage            
            self.data_storage.add_resource(
                resource=Resource(
                    label=self.label, #resource takes label name equal to Source.label
                    data=f.read(),
                    file_type=chosen_ext
                )
            )

        converter = Converter()

        #Datetime handling
        #We are going to deal with standardizing the datetimes found in the 
        #Resource, why directly the resource? Cause we are guaranteed that 
        #we have a pd.DataFrame.
        dt_parser       = DatetimeParser()
        r_dt_to_replace = self.data_storage.get_resource(label=self.label).get_data()
        r_dt_to_replace = converter.to_data_format(data=r_dt_to_replace, data_format='dataframe')
        r_dt_to_replace = dt_parser.parse_batch(r_dt_to_replace)
        self.data_storage.replace_resource(
            new_resource=Resource(
                label=self.label,
                data=converter.to_resource_format(data=r_dt_to_replace, file_format=chosen_ext, details='UpdateResource'),
                file_type=chosen_ext))

        #Dynamic column(s) handling:
        #   Once a Source component is istanciated, columns can be took in account or not
        #   Here we get them, if istanciated, otherwise an automatic association is done, 
        #   Using Levenshtein distance algorithm. 
        #   Remember that this component is multi-purpose, used both for Input data for models
        #   and also for Validation data.
        if self.columns == 'auto':
            #If we have 'auto', we need to get the data as pandas dataframe and catch
            #the columns, so we need to convert from whatever is the input format.
            r_data       = self.data_storage.get_resource(label=self.label).get_data()  
            r_data_df    = converter.to_data_format(data=r_data, data_format="dataframe")
            auto_columns = r_data_df.columns.to_list()
        detect_engine   = LevenshteinDistanceColumnDetectEngine(duplicate_policy='best')
        columns_mapping = \
            self.get_columns(
                engine=detect_engine, 
                columns=auto_columns if self.columns == 'auto' else self.columns,
                detect_type='auto' if self.columns == 'auto' else 'specified')

        #drop mapping into MosaicSharedMemory, right now is not used in the current
        #pipeline but maybe it's gonna be find a use for it.
        global_columns_sources_mapping = self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='pass')
        if global_columns_sources_mapping == None:
            self.shared_memory.add_variable(key='global_columns_sources_mapping', content={self.label:columns_mapping}, is_immutable=False)
        else:
            global_columns_sources_mapping = self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='raise').content
            global_columns_sources_mapping[self.label] = columns_mapping
            self.shared_memory.update_variable(key='global_columns_sources_mapping', new_content=global_columns_sources_mapping)
        global_columns_sources_mapping = self.shared_memory.get_variable(key='global_columns_sources_mapping', error_policy='raise').content
        #Input validation
        input_data_df        = converter.to_data_format(data=self.data_storage.get_resource(label=self.label).get_data(), data_format="dataframe")
        #Here we need to get if a dataframe is fillable, 
        #we define a dataframe as fillable if it has no GenericColumn in it
        #GenericColumn means that we have not addressable columns 
        #like in the case of validation or whatever processing we are considering
        #other than an input data processing 
        is_fillable          = all([_class!='GenericColumn' for col, _class in columns_mapping.items()])
        print(f"[Source] Data {'is fillable' if is_fillable else 'is not fillable. Skipping filling process.'}.")
        print(f"[Source] Details: {[_class=='GenericColumn' for col, _class in columns_mapping.items()]}.")
        input_data_validator = InputDataValidator(data=input_data_df)
        missing_records = input_data_validator.run() \
            if is_fillable \
            else {'missing_data_points' : [], 'details' : None}
        coordinates_variable = self.shared_memory.get_variable(key='geospatial_data', error_policy = 'pass')
        if coordinates_variable!= None:
            input_data_filler    = InputDataFiller(
                coordinates=coordinates_variable, 
                weather_parameters_mapping=global_columns_sources_mapping[self.label], 
                lat=coordinates_variable.content['latitude'],
                long=coordinates_variable.content['longitude'])
        else:
            input_data_filler    = InputDataFiller(
                coordinates=coordinates_variable, 
                weather_parameters_mapping=global_columns_sources_mapping[self.label])
        filled_df            = input_data_filler.fill(data=input_data_df, missing_data=missing_records)
        #Now we get to update the resource with the filled data, 
        #replacing it entirely.
        self.data_storage.replace_resource(
            new_resource=Resource(
                label=self.label,
                data=converter.to_resource_format(data=filled_df, file_format=chosen_ext, details='UpdateResource'),
                file_type=chosen_ext))
        
        self.file = file + "." + chosen_ext
        print("[Source]: Data 'local' dumped into MosaicDataStorage.")
        print("[Source]: Closed running.")
        return
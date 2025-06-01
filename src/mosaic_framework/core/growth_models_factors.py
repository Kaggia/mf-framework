################################################################################
# Module: growth_models_factors.py
# Description: Implementation of growth models related rules.
# Author:      Stefano Zimmitti
# Date: 14/06/2024
# Company: xFarm Technologies
################################################################################

import pandas as pd
from copy import deepcopy
from typing import List, Dict, AnyStr, Any
import dateutil

from mosaic_framework.core.agronomical_factors import AgroRule
from mosaic_framework.core.exceptions import ColumnNameError, DataFormatException

class DayOfYear(AgroRule):
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
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for the DayOfYear rule.
        Check if the targeted column is a date-like string column.

        Args:
            data (pd.DataFrame): Input dataframe to prepare

        Returns:
            pd.DataFrame: Prepared dataframe

        Raises:
            DataFormatException: If target column is not in date string format
        """
        def is_eligible(date_str:str) -> bool:
            try:
                pd.to_datetime(date_str, errors='raise')
                return True
            except ValueError:
                return False

        #check wether the targeted column is an eligible date-str column
        if not all(data[self.target].apply(is_eligible).to_list()):
            raise DataFormatException(f"{self.target} format is not what expected: Date string-like needed.")

        return super().prepare(data)
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the DayOfYear rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe to process

        Returns:
            pd.DataFrame: Processed dataframe with day of year calculations added
        """
        def compute_doy(date:str) -> int:
            """
            Compute the day of year from a date string. Uses the dateutil library.

            Args:
                date (str): Date string to process

            Returns:
                int: Day of year (1-366)
            """
            return dateutil.parser.parse(date).timetuple().tm_yday
        
        AgroRule.evaluate(self, data=data)
        data    = self.prepare(data)
        updt_df = deepcopy(data)

        updt_df[self.column] = updt_df.apply(lambda row: compute_doy(row[self.target]), axis=1)

        return self.finalize(data=updt_df)

class GDD(AgroRule):
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
        min_temp (float): Minimum temperature threshold for the culture.
        max_temp (float): Maximum temperature threshold for the culture.
    """

    def __init__(self, min_temp:float, max_temp:float, cumulate:bool=False, start_doy:int=1, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.min_temp     = min_temp
        self.max_temp     = max_temp
        self.cumulate     = cumulate
        self.start_doy    = start_doy
    
    def get_target_column(self, data: pd.DataFrame) -> Dict[AnyStr, AnyStr]:
        """
        Get the target column for the GDD rule.
        The target column is the column containing the minimum and maximum temperature data.

        Args:
            data (pd.DataFrame): Input dataframe to analyze

        Returns:
            Dict[AnyStr, AnyStr]: Dictionary containing the detected column names for min_temp, max_temp and sample_date

        Raises:
            ColumnNameError: If required columns are not found
        """
        
        target   = {'min_temp': None, 'max_temp': None, 'sample_date': None}
        list_min = ['min_temperature', 'min_temp', 't_min', 'temp_min', 'temperature_min', 'min_t', 'Tmin']
        list_max = ['max_temperature', 'max_temp', 't_max', 'temp_max', 'temperature_max', 'max_t', 'Tmax']
        list_date = ['sampleDate', 'sample_date', 'date', 'datetime']
        for possible_col_name in list_min:
            if possible_col_name in data.columns:
                target['min_temp']=possible_col_name
                break
        for possible_col_name in list_max:
            if possible_col_name in data.columns:
                target['max_temp']=possible_col_name
                break
        
        for possible_col_name in list_date:
            if possible_col_name in data.columns:
                target['sample_date']=possible_col_name
                break
        
        if target['min_temp']==None \
            or target['max_temp']==None \
            or target['sample_date']==None:
            raise ColumnNameError(f"Columns are not been recognized or they are absent. Check Minimum and Maximum or Sample Date.")
        return target
    
    def get_doy(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Get the day of year for the GDD rule.
        """
        # Calculate day of year using pandas datetime
        data['doy_to_remove'] = pd.to_datetime(data[self.get_target_column(data)['sample_date']]).dt.dayofyear
        return data
    
    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for the GDD rule.
        """
        data        = super().prepare(data)
        data        = self.get_doy(data=data)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the GDD rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe to process

        Returns:
            pd.DataFrame: Processed dataframe with GDD calculations added
        """
        def compute_GDD(t_max:float, t_min:float, temp_colture_min:float, doy:int) -> float:
            """
            Compute the Growing Degree Day (GDD) based on the minimum and maximum temperature data and the colture data.

            Args:
                t_max (float): Maximum temperature
                t_avg (float): Average temperature  
                temp_colture_max (float): Maximum temperature threshold for the culture
                temp_colture_min (float): Minimum temperature threshold for the culture

            Returns:
                float: Calculated GDD value
            """

            media = (t_max + t_min)/2
            gdd   = media - temp_colture_min
            if gdd <= 0:
                gdd = 0
            return gdd if doy >= self.start_doy else 0
        
        AgroRule.evaluate(self, data=data)
        data    = self.prepare(data)
        updt_df = deepcopy(data)

        detected_columns     = self.get_target_column(data=data)

        # Compute GDD for each row
        
        updt_df[self.column] = updt_df.apply(\
            lambda row: compute_GDD(
                t_max=row[detected_columns['max_temp']], 
                t_min=row[detected_columns['min_temp']], 
                doy=row['doy_to_remove'],
                temp_colture_min=self.min_temp), axis=1)
        
        #If cumulate is True, compute the cumulative sum of the GDD
        if self.cumulate:
            updt_df['temp_cum']  = updt_df[self.column].cumsum()
            updt_df[self.column] = updt_df['temp_cum']
            updt_df.drop('temp_cum',      axis=1, inplace=True)
            updt_df.drop('doy_to_remove', axis=1, inplace=True)

        return self.finalize(data=updt_df)

class ActualPhenostageData(AgroRule):
    """
    This class represents a generic comparative rule.
    It takes a target column, a column to compare, and a condition.
    It applies the condition to the target column and returns a new dataframe with the result.

    Args:
        parameter (str): Parameter to extract from phenological stage data
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        on_condition (object, optional): The condition for the agronomical factor. Defaults to None.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """

    def __init__(self, parameter:str, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.parameter     = self._get_valid_parameter(parameter=parameter)
        self.default_value = kwargs.get('default_value', -1)
    
    def _get_valid_parameter(self, parameter:str) -> str:
        """
        Found the correct mapping for the parameter.

        Args:
            parameter (str): Parameter name to validate

        Returns:
            str: Validated parameter name

        Raises:
            DataFormatException: If parameter is not found in mapping
        """
        mapping = {
            'pheno_phase_id'   : ['id'   , 'phase_id'   , 'pheno_phase_id'   , 'phenological_phase_id'] ,
            'pheno_phase_name' : ['name' , 'phase_name' , 'pheno_phase_name' , 'phenological_phase_name'],
            'pheno_phase_start': ['start', 'phase_start', 'pheno_phase_start', 'phenological_phase_start'],
            'pheno_phase_end'  : ['end'  , 'phase_end'  , 'pheno_phase_end'  , 'phenological_phase_end'],
            'pheno_phase_unit' : ['unit' , 'phase_unit' , 'pheno_phase_unit' , 'phenological_phase_unit']
        }

        # Create reverse mapping from any valid parameter name to canonical name
        reverse_mapping = {alias: key for key, aliases in mapping.items() for alias in aliases}
        
        if parameter not in reverse_mapping:
            valid_params = list(reverse_mapping.keys())
            raise DataFormatException(f"Parameter {parameter} not found in mapping. Valid parameters are: {valid_params}")
            
        return reverse_mapping[parameter]
    
    def get_growth_model(self) -> List[Dict]:
        """
        Get the growth model from the v_look_up_table.

        Returns:
            List[Dict]: Growth model data from lookup table

        Raises:
            DataFormatException: If VLookUpTable or growth_model not found
        """
        #Get VLookUpTable
        v_look_up_table = self.rules_hub.get_variable('v_look_up_table')
        if v_look_up_table is None:
            raise DataFormatException(f"VLookUpTable has not been found.")
        #Get GrowthModel
        growth_model    = v_look_up_table.content.get('growth_model', None)
        if growth_model is None:
            raise DataFormatException(f"GrowthModel is not been found in VLookUpTable.")
        return growth_model
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate the ActualPhenostage rule on the input data.

        Args:
            data (pd.DataFrame): Input dataframe to process

        Returns:
            pd.DataFrame: Processed dataframe with phenological stage data added
        """
        def get_actual_phase_data(value: Any, growth_model: List[Dict], parameter:str) -> int:
            for phase in growth_model:
                start = phase['pheno_phase_start']
                end   = phase['pheno_phase_end']
                
                # Try to convert to dates if datelike
                try:
                    if isinstance(value, str):
                        value = pd.to_datetime(value)
                    if isinstance(start, str):
                        start = pd.to_datetime(start)
                    if isinstance(end, str):
                        end = pd.to_datetime(end)
                except:
                    # If conversion fails, treat as float
                    value = float(value)
                    start = float(start) 
                    end   = float(end)

                if start <= value <= end:
                    return phase[parameter]
                    
            return self.default_value
        AgroRule.evaluate(self, data=data)
        data    = self.prepare(data)
        updt_df = deepcopy(data)

        growth_model = self.get_growth_model()

        #get the actual phase data parameter, 
        #by selecting the phase that contains the gdd value
        updt_df[self.column] = updt_df.apply(
            lambda row: get_actual_phase_data(
                value=row[self.target],
                growth_model=growth_model,
                parameter=self.parameter
            ), axis=1)

        return self.finalize(data=updt_df)

class ActualPhenostageId(ActualPhenostageData):
    """
    This class represents a phenological stage identifier.
    It inherits from ActualPhenostageData and specifically returns the phase_id parameter
    from the growth model for the given target value.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), parameter='phase_id', is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))

class ActualPhenostageName(ActualPhenostageData):
    """
    This class represents a phenological stage identifier.
    It inherits from ActualPhenostageData and specifically returns the phase_name parameter
    from the growth model for the given target value.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), parameter='phase_name', is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))

class ActualPhenostageStart(ActualPhenostageData):
    """
    This class represents a phenological stage identifier.
    It inherits from ActualPhenostageData and specifically returns the phase_start parameter
    from the growth model for the given target value.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), parameter='phase_start', is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))

class ActualPhenostageEnd(ActualPhenostageData):
    """
    This class represents a phenological stage identifier.
    It inherits from ActualPhenostageData and specifically returns the phase_end parameter
    from the growth model for the given target value.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), parameter='phase_end', is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))

class ActualPhenostageUnit(ActualPhenostageData):
    """
    This class represents a phenological stage identifier.
    It inherits from ActualPhenostageData and specifically returns the phase_unit parameter
    from the growth model for the given target value.

    Args:
        target (str): The target column name.
        column (str): The column name for the agronomical factor.
        is_implicit (bool): Whether the column is implicit or not.
        debug (bool, optional): Whether to print debug information or not. Defaults to False.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(column=kwargs.get('column', None), target=kwargs.get('target', None), parameter='phase_unit', is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
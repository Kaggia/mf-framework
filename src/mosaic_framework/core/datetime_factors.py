################################################################################
# Module: comparative_factors.py
# Description: Implementation of comparative factors.
# Author:     Stefano Zimmitti, Lorenzo Gini
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from typing import List, Union, Dict
from mosaic_framework.core.exceptions import DataFormatException
from mosaic_framework.core.agronomical_factors import AgroRule
import pandas as pd 

class getHourRule(AgroRule):
    """
    Extract the hour from a datetime column and create a new column with the specified name.

    Args:
        target (str): The target datetime column name
        column (str): The output column name for the hour value
        is_implicit (bool): Whether the column is implicit
        debug (bool): Enable debug mode
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None),column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the data for evaluation.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Prepared dataframe
        """
        return super().prepare(data)
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Extract hour from datetime column.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with hour column added

        Raises:
            DataFormatException: If target column cannot be converted to datetime
        """
        updt_df = AgroRule.evaluate(self, data=data)
        updt_df = self.prepare(updt_df)
        try:
            updt_df['date_in_datetime'] = pd.to_datetime(updt_df[self.target])
        except:
            raise DataFormatException('The target column can not be converted in Timespamp.')
        updt_df[self.column] = updt_df['date_in_datetime'].apply(lambda x: x.hour)
        updt_df.drop(columns=['date_in_datetime'], inplace=True)
        return self.finalize(data=updt_df)

class getDayRule(AgroRule):
    """
    Extract the day from a datetime column and create a new column with the specified name.

    Args:
        target (str): The target datetime column name
        column (str): The output column name for the day value
        is_implicit (bool): Whether the column is implicit
        debug (bool): Enable debug mode
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None),column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Extract day from datetime column.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with day column added

        Raises:
            DataFormatException: If target column cannot be converted to datetime
        """
        updt_df = AgroRule.evaluate(self, data=data)
        updt_df = self.prepare(updt_df)
        try:
            updt_df['date_in_datetime'] = pd.to_datetime(updt_df[self.target])
        except:
            raise DataFormatException('The target column can not be converted in Timespamp.')
        updt_df[self.column] = updt_df['date_in_datetime'].apply(lambda x: x.day)
        updt_df.drop(columns=['date_in_datetime'], inplace=True)
        return self.finalize(data=updt_df)

class isNightTimeRule(AgroRule):
    """
    Create a binary column indicating if time is outside specified range (night time).
    Output is 1 if time is outside range, 0 if within range.

    Args:
        range (List[Union[int, int]]): Hour range defining day time [start_hour, end_hour]
        target (str): The target datetime column name
        column (str): The output column name for the binary indicator
        is_implicit (bool): Whether the column is implicit
        debug (bool): Enable debug mode
    """
    def __init__(self, range:List[Union[int, int]], **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None),column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.range = range

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Extract hour from datetime for evaluation.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with hour column added
        """
        data = super().prepare(data=data)
        gHR  = getHourRule(target=self.target, column='columns_to_delete')
        gHR.set_rules_hub(rules_hub=self.rules_hub)
        data = gHR.evaluate(data=data)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Create binary night time indicator column.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with night time indicator column
        """
        def const(x):
            if min(self.range) <= x <= max(self.range):
                return 0
            else:
                return 1
        
        updt_df = AgroRule.evaluate(self, data=data)
        updt_df = self.prepare(updt_df)
        updt_df[self.column] = updt_df['columns_to_delete'].apply(const)
        updt_df = updt_df.drop(columns=['columns_to_delete'])
        return self.finalize(data=updt_df)
    
class isDayTimeRule(AgroRule):
    """
    Create a binary column indicating if time is within specified range (day time).
    Output is 1 if time is within range, 0 if outside range.

    Args:
        range (List[Union[int, int]]): Hour range defining day time [start_hour, end_hour]
        target (str): The target datetime column name
        column (str): The output column name for the binary indicator
        is_implicit (bool): Whether the column is implicit
        debug (bool): Enable debug mode
    """
    def __init__(self, range:List[Union[int, int]], **kwargs) -> None:
        super().__init__(target=kwargs.get('target', None),column=kwargs.get('column', None), is_implicit=kwargs.get('is_implicit', False), debug=kwargs.get('debug', False))
        self.range = range

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Extract hour from datetime for evaluation.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with hour column added
        """
        data = super().prepare(data=data)
        gHR  = getHourRule(target=self.target, column='columns_to_delete')
        gHR.set_rules_hub(rules_hub=self.rules_hub)
        data = gHR.evaluate(data=data)
        return data
    
    def evaluate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Create binary day time indicator column.

        Args:
            data (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Dataframe with day time indicator column
        """
        def const(x):
            if min(self.range) <= x <= max(self.range):
                return 1
            else:
                return 0
        
        updt_df = AgroRule.evaluate(self, data=data)
        updt_df = self.prepare(updt_df)
        updt_df[self.column] = updt_df['columns_to_delete'].apply(const)
        updt_df = updt_df.drop(columns=['columns_to_delete'])
        return self.finalize(data=updt_df)
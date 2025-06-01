################################################################################
# Module:      datetime_parser.py
# Description: Module used to validate and convert a batch of datetimes or 
#              a single one.
# Author:      Stefano Zimmitti
# Date:        07/10/2024
# Company:     xFarm Technologies
################################################################################

import datetime
import logging
from typing import  List, Any
from dateutil import parser
from datetime import timezone
import pandas as pd

from mosaic_framework.dt.exceptions import DateParsingException

class DatetimeParser():
    """    
    This class helps to validate and convert a batch of datetimes or
    a single one. The idea is to chose a unique standard to ease the work 
    around datetimes. We are going to use the ISO 8601 standard. So anytime 
    we need to be sure that we are dealing with the correct format, we can
    validate and safely convert the datetime to a string using the standard.
    \nThe standard is the following:
    YYYY-MM-DDTHH:MM:SS.mmmmmm+HH:MM
    With each parsing operation first we assure that we are dealing with a 
    valid input datetime (or datetimes) than we convert it to the standard.
    """

    def __init__(self, output_format: str = "%Y-%m-%dT%H:%M:%S.%f%z", iso_std: str = 'iso_8601') -> None:
        self.output_format = output_format
        self.iso_std = iso_std

    @staticmethod
    def __parse_list(l: list) -> List[str]:
        """
        Parse a list of dates.
        """
        return [DatetimeParser().parse_single(d) for d in l]
    
    @staticmethod
    def __parse_dict(d: dict) -> dict:
        """
        Parse a dictionary of dates.
        """
        for k in d.keys():
            if not isinstance(d[k], dict):
                # If the value is not a string, try to parse it
                try:
                    d[k] = DatetimeParser().parse_single(d[k])
                except DateParsingException:
                    raise DateParsingException(f"Error parsing date for key {k}")
            else:
                raise DateParsingException("Nested dictionaries are not supported")
        return d
    
    @staticmethod
    def __parse_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a pandas DataFrame of dates.
        """
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]) or pd.api.types.is_object_dtype(df[c]):
                df[c] = df[c].apply(lambda x: DatetimeParser().parse_single(x) if pd.notnull(x) else x)
        return df

    @staticmethod
    def parse_batch(b: Any) -> Any:
        """
        Parse dates as batch. Available batch formats: list, dict, pandas.DataFrame.
        - list:             list of date-like strings.
        - dict:             dict with at least one key as string and value as string.
        - pandas.DataFrame: DataFrame with at least one column as datelike.
        """
        parser_fnc = {
            list: DatetimeParser.__parse_list,
            dict: DatetimeParser.__parse_dict,
            pd.DataFrame: DatetimeParser.__parse_dataframe
        }

        fnc = parser_fnc.get(type(b), None)

        if fnc is None:
            raise DateParsingException("Invalid batch format or no function associated with that type.")
        
        return fnc(b)
    
    def parse_single(self, d: str) -> str:
        """
        parse a single string, standardizing it to ISO8601

        Args:
            d (str): datetime as string.

        Raises:
            DateParsingException: Not valid datetime format

        Returns:
            str: Standardized datetime expressed as string.
        """
        formatted_date = None
        
        # Check on the date
        try:
            # Try to parse
            parsed_date = parser.parse(d)
            
            # If the date is naive (no timezone), add UTC as default timezone
            if parsed_date.tzinfo is None:
                parsed_date = parsed_date.replace(tzinfo=timezone.utc)

            # Format the date in the most complete format:
            formatted_date = parsed_date.strftime(self.output_format)
        except (ValueError, TypeError):
            raise DateParsingException(f"{type(d)}: {d} has not a valid date format.")
        
        return formatted_date

    def get_standard_datetime(self, d:str)-> datetime.datetime:
        """Get standardized datetime.datetime, with ISO8601, from a string.

        Args:
            d (str): datetime as string.

        Returns:
            datetime.datetime: ISO8601 formatted datetime.
        """
        return datetime.datetime.strptime(d, self.output_format)
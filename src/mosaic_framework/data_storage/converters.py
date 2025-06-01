################################################################################
# Module:      convertion.py
# Description: Module used to convert data from a format to another.
# Author:      Stefano Zimmitti
# Date:        21/08/2024
# Company:     xFarm Technologies
################################################################################

from io import BytesIO
from typing import Any
import pandas as pd
import json

from mosaic_framework.config.configuration import CONVERTERS_MAPPING
from mosaic_framework.data_storage.exceptions import ConvertionException

class Converter:
    """
    Class used to convert data from a format to another. It is possible to
    use both convertion to resource format (like a file, in the storage) or
    convertion to data format (like a pandas DataFrame).
    The class is used to manage the convertion and to provide a generic interface.

    Args:
        None
    
    Methods:
        convert_from_dataframe_to_records(data:pd.DataFrame): Converts a pandas DataFrame to a list of dictionaries.
        convert_from_dataframe_to_dataframe(data:pd.DataFrame): Forward the input, for the sake of generality.
        get_method_name(data:Any, mapping_out_key:str, output_type:str): Returns the method name to be used for conversion.
        to_resource_format(data:Any, file_format:str): Converts the data to the specified file format.
        to_data_format(data:Any, data_format:str): Converts the data to the specified data format.
    """

    def __init__(self):
        pass
    
    @staticmethod
    def convert_from_json_to_dataframe(data:Any):
        """
        Converts from a json to a dataframe. Checking the input if it expressed as a list or dict.
        """
        if isinstance(data, dict):
            if not 'data' in list(data.keys()):
                raise ConvertionException("Cannot find the key 'data' in the JSON provided.")
            else:
                data = data['data']
        elif isinstance(data, list):
            if not all(isinstance(item, dict) for item in data):
                raise ConvertionException("data does not contain all dictionaries.")
            else:
                data = data
        else:
            raise ConvertionException("Unsupported data type.")
        
        return pd.DataFrame(data=data)

    @staticmethod
    def convert_from_dataframe_to_records(data:pd.DataFrame):
        """
        Converts a pandas DataFrame to a list of dictionaries.
        """
        return data.to_dict('records')
    
    @staticmethod
    def convert_from_dataframe_to_json(data:pd.DataFrame):
        """
        Converts a pandas DataFrame to a list of dictionaries.
        """
        return json.dumps(data.to_dict('records'), default=str).encode('utf-8')

    @staticmethod
    def convert_from_dataframe_to_dataframe(data:pd.DataFrame):
        """
        Forward the input, for the sake of generality.
        """

        return data

    @staticmethod
    def convert_from_dataframe_to_xlsx(data:pd.DataFrame):
        """
        Converts a pandas DataFrame in a file Excel (.xlsx) in memory.

        Args:
        - data (pd.DataFrame): Dataframe to convert.
        - sheet_name (str) : Name of the sheet in the Excel file

        Returns:
        - excel_buffer (BytesIO): In memory buffer containing the Excel file.
        """
        buffer = BytesIO()
        
        # Scriviamo il DataFrame nel buffer come file Excel
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            data.to_excel(writer, index=False, sheet_name='Foglio1')
        
        # Riportiamo il puntatore all'inizio del buffer
        buffer.seek(0)
        
        # Creiamo un oggetto ExcelFile direttamente dal buffer
        excel_file = pd.ExcelFile(buffer)
        
        return excel_file

    @staticmethod
    def get_method_name(data:Any, mapping_out_key:str, output_type:str):
        """
        Returns the method name to be used for conversion.
        """
        input_type   = str(type(data))[str(type(data)).find("'")+1:str(type(data)).rfind("'")] if "'" in str(type(data)) else str(type(data))
        input_format = CONVERTERS_MAPPING.get('types', None).get(input_type, None)
        output_format= CONVERTERS_MAPPING.get(mapping_out_key, None).get(output_type, None)
        return f"convert_from_{input_format}_to_{output_format}"
    
    def to_resource_format(self, data:Any, file_format:str, details:str=""):
        """
        Converts the data to the specified file format. Useful to be dumped in a file right for it.
        """
        converted_data = None
        method_name = self.get_method_name(data=data, mapping_out_key='extensions', output_type=file_format)
        print(f"[Converter|to:{file_format}||{'|'+details if details !='' else ''}] Converting method: {method_name}")
        if hasattr(self, method_name):
            convert_method = getattr(self, method_name)
            converted_data = convert_method(data=data)
        else:
            raise ConvertionException(f"Cannot find the method {method_name}.")
        
        if converted_data is None:
            raise ConvertionException(f"Cannot convert data to {file_format}.")
        
        return converted_data
    
    def to_data_format(self, data:Any, data_format:str):
        """
        Converts the data to the specified file format.
        """
        converted_data = None
        method_name = self.get_method_name(data=data, mapping_out_key='types', output_type=data_format)
        print(f"[Converter] Converting method: {method_name}")
        if hasattr(self, method_name):
            convert_method = getattr(self, method_name)
            converted_data = convert_method(data=data)
        else:
            raise ConvertionException(f"Cannot find the method {method_name}.")
        
        if converted_data is None:
            raise ConvertionException(f"Cannot convert data to {data_format}.")
        
        return converted_data

################################################################################
# Module:      writers.py
# Description: Implementation of writers, let the data persist in MosaicDataStorage
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import Protocol, TYPE_CHECKING
import json
import pandas as pd

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage

from mosaic_framework.data_storage.exceptions import DataFormatException

class ProtocolReader(Protocol):
    def persist(self):
        ...

class TextReader(ProtocolReader):
    """
    Implementing a text file reader.
    """
    def __init__(self, data_storage: MosaicDataStorage) -> None:
        """
        Initialize the TextReader with a given MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object to read from.
        ---\n
        returns: None
        """
        self.data_storage = data_storage
        self.ext = ".txt"

    def read(self, label: str) -> object:
        """
        Read data from a file in the MosaicDataStorage, in text format.
        ---\n
        params:
        label: str - The filename (without extension).
        ---\n
        returns: 
        List[str] or str - The data read from the file.
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        with open(filepath, "r+") as f:
            data = f.readlines()
            if len(data) == 1: 
                data = data[0]
        return data

class JsonReader(ProtocolReader):
    """
    Implementing a JSON file reader.
    """
    def __init__(self, data_storage: MosaicDataStorage) -> None:
        """
        Initialize the JsonReader with a given MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object to read from.
        ---\n
        returns: None
        """
        self.data_storage = data_storage
        self.ext = ".json"

    def read(self, label: str) -> dict:
        """
        Read data from a file in the MosaicDataStorage, in JSON format.
        ---\n
        params:
        label: str - The filename (without extension).
        ---\n
        returns: 
        dict - The data read from the file in raw format.
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        with open(filepath, "r+") as f:
            data = json.load(f)
            #If 'data' is in the list of keys, then we need to
            #point to that key and return the content
            if isinstance(data, dict):
                if 'data' in list(data.keys()):
                    data = data['data']
            elif isinstance(data, list):
                data = data
            else: 
                raise DataFormatException(f'Data format is not what expected: found: {type(data)} | expected: [list | dict]')
        return data

class CsvReader(ProtocolReader):
    """
    Implementing a CSV file reader to return more structured data, such as a pandas DataFrame.
    """
    def __init__(self, data_storage: MosaicDataStorage) -> None:
        """
        Initialize the CsvReader with a given MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object to read from.
        ---\n
        returns: None
        """
        self.data_storage = data_storage
        self.ext = ".csv"

    def read(self, label: str) -> pd.DataFrame:
        """
        Read data from a file in the MosaicDataStorage, in CSV format.
        Returning a pandas DataFrame.
        ---\n
        params:
        label: str - The filename (without extension).
        ---\n
        returns: 
        pd.DataFrame - The data read from the file.
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        return pd.read_csv(filepath)

class ExcelReader(ProtocolReader):
    """
    Implementing an Excel file reader to return more structured data, such as a pandas DataFrame.
    """
    def __init__(self, data_storage: MosaicDataStorage) -> None:
        """
        Initialize the ExcelReader with a given MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object to read from.
        ---\n
        returns: None
        """
        self.data_storage = data_storage
        self.ext = ".xlsx"

    def read(self, label: str) -> pd.DataFrame:
        """
        Read data from a file in the MosaicDataStorage, in Excel format.
        Returning a pandas DataFrame.
        ---\n
        params:
        label: str - The filename (without extension).
        ---\n
        returns: 
        pd.DataFrame - The data read from the file.
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        return pd.read_excel(io=filepath, sheet_name=pd.ExcelFile(filepath).sheet_names[0], engine='openpyxl')

class PyReader(ProtocolReader):
    """
    Implementing a text file reader.
    """
    def __init__(self, data_storage: MosaicDataStorage) -> None:
        """
        Initialize the PyReader with a given MosaicDataStorage.
        ---\n
        params:
        data_storage: MosaicDataStorage - The data storage object to read from.
        ---\n
        returns: None
        """
        self.data_storage = data_storage
        self.ext = ".py"

    def read(self, label: str) -> object:
        """
        Read data from a file in the MosaicDataStorage, in py format.
        ---\n
        params:
        label: str - The filename (without extension).
        ---\n
        returns: 
        List[str] or str - The data read from the file.
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        with open(filepath, "r+") as f:
            data = f.read()
        return data

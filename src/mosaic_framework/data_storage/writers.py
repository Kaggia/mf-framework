################################################################################
# Module:      writers.py
# Description: Implementation of writers, let the data persist in MosaicDataStorage
# Author:      Stefano Zimmitti
# Date:        17/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import Protocol,TYPE_CHECKING
import json
import pandas as pd

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage

class ProtocolWriter(Protocol):
    def persist(self):
        ...

class TextWriter(ProtocolWriter):
    """
    Implementing a txt writer file.
    """
    def __init__(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage   = data_storage
        self.ext            = ".txt"

    
    def persist(self, label:str, data:object)->bool:
        """
        Write 'data' in a file, inside a MosaicDataStorage, in txt format.
        ---\n
        params:
        label(str): filename (without extension),
        data(str) : content of file to write.
        ---\n
        returns: Boolean value (True) wether or not file is written
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        f = open(filepath, "w+")
        f.write(data)
        f.close()
        print(f"[TextWriter]: {filepath} written.")
        return True

class PyWriter(ProtocolWriter):
    """
    Implementing a py writer file.
    """
    def __init__(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage   = data_storage
        self.ext            = ".py"

    
    def persist(self, label:str, data:object)->bool:
        """
        Write 'data' in a file, inside a MosaicDataStorage, in txt format.
        ---\n
        params:
        label(str): filename (without extension),
        data(str) : content of file to write.
        ---\n
        returns: Boolean value (True) wether or not file is written
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        f = open(filepath, "w+", encoding='utf-8')
        f.write(data)
        f.close()
        print(f"[TextWriter]: {filepath} written.")
        return True


class JsonWriter(ProtocolWriter):
    """
    Implementing a json writer file.
    """
    def __init__(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage   = data_storage
        self.ext            = ".json"

    def persist(self, label:str, data:object)->bool:
        """
        Write 'data' in a file, inside a MosaicDataStorage, in json format.
        ---\n
        params:
        label(str): filename (without extension),
        data(str) : content of file to write.
        ---\n
        returns: Boolean value (True) wether or not file is written
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        
        with open(filepath, 'wb') as file:
            file.write(data)

        print(f"[JsonWriter]: {filepath} written.")
        return True

class CsvWriter(ProtocolWriter):
    """
    Implementing a csv writer file.
    """
    def __init__(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage   = data_storage
        self.ext            = ".csv"

    def persist(self, label:str, data:object)->bool:
        """
        Write 'data' in a file, inside a MosaicDataStorage, in csv format. Can be written
        both bytes or pd.DataFrame.
        ---\n
        params:
        label(str): filename (without extension),
        data(str) : content of file to write.
        ---\n
        returns: Boolean value (True) wether or not file is written
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        
        with open(filepath, 'wb') as file:
            if isinstance(data, pd.DataFrame):
                data.to_csv(filepath)
                print(f"Printed csv: {filepath}")
            else:
                file.write(data)

        print(f"[CsvWriter]: {filepath} written.")
        return True

class ExcelWriter(ProtocolWriter):
    """
    Implementing a xlsx writer file.
    """
    def __init__(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage   = data_storage
        self.ext            = ".xlsx"

    def persist(self, label:str, data:object)->bool:
        """
        Write 'data' in a file, inside a MosaicDataStorage, in csv format.
        ---\n
        params:
        label(str): filename (without extension),
        data(str) : content of file to write.
        ---\n
        returns: Boolean value (True) wether or not file is written
        """
        filepath = self.data_storage.path + "/" + label + self.ext
        
        
        #here we can have a Buffer pointer or directly byte-like
        if isinstance(data, pd.ExcelFile):
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name in data.sheet_names:
                    # Leggi il DataFrame da ogni foglio e scrivilo nel nuovo file
                    df_sheet = data.parse(sheet_name)
                    df_sheet.to_excel(writer, index=False, sheet_name=sheet_name)
            print(f"[XlsxWriter]: Printed with ExcelWriter: {filepath}")
        else:
            with open(filepath, 'wb') as file:
                file.write(data)
            print(f"[XlsxWriter]: Printed with ByteWriter: {filepath}")

            

        print(f"[XlsxWriter]: {filepath} written.")
        return True

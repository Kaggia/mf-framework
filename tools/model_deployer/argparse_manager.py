################################################################################
# Module: argparse_manager.py
# Description: Managing argument parsing.
# Author: Simone Di Domenico, Stefano Zimmitti
# Date: 15/03/2023
# Company: xFarmTech
################################################################################
import argparse
import datetime

class ArgParser():
    def __init__(self, options:list) -> None:
        self.options = options
        self.data = dict()
        
        # Define command line options
        # this also generates --help and error handling
        self.CLI=argparse.ArgumentParser()

        #Add all needed args
        self.__add_args(options=self.options)

    
    #For each option got in input, define a new argument
    #options: list of <Option>
    def __add_args(self, options:list):
        for option in options:
            for i in range(len(option._name)):
                try:
                    self.CLI.add_argument(
                        f"-{option._name[0:i+1]}", 
                        f"--{option._name}", 
                        dest=f"{option._name}",
                        nargs='+', 
                        default=option.default)
                    print(f"<Option:-{option._name[0:1]}||--{option._name}> loaded.")
                    break
                except argparse.ArgumentError:
                    pass
                self.data[f"{option._name}"] = None
         
    def get_parsed_args(self):
        for k in self.CLI.parse_args().__dict__:
            if self.CLI.parse_args().__dict__[k] is not None:
                self.data[k] = self.CLI.parse_args().__dict__[k]
            if isinstance(self.data[k], list):
                self.data[k] = self.data[k][0]
            if str(self.data[k]).isnumeric():
                self.data[k] = int(self.data[k])
            self.data[k] = self.__deep_parsing(v=self.data[k])
        print(f"\n\n\n[ArgParser] Parsed arguments-> ", self.data, "\n\n\n")
        return self.data
    
    #Parse eventual values (bool)
    def __deep_parsing(self, v):
        #check if True
        if v == "true" or v == "True":
            v = True
        if v == "false" or v == "False":
            v = False
        return v

class RegionID():
    def __init__(self) -> None:
        self.default = 119

class Option():
    def __init__(self, n:str, t:type, d:object) -> None:
        self._name = n
        self._type = t
        self.default = d
    

    #set default value based on type assigned
    def set_default(self):
        def_value = None
        if self._type == datetime.datetime:
            #But it is a string like: 2022-06-10 YYYY-mm-dd
            def_value = datetime.datetime.today().strftime("%Y-%m-%d")
        if self._type == RegionID:
            nrid = RegionID()
            def_value = nrid.default
        if self._type == bool:
            def_value = True
        return def_value




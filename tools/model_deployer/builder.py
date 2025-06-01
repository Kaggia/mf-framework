################################################################################
# Module:      builder.py
# Description: Got template and model data, build a new mosaic pipeline file.
# Author:      Stefano Zimmitti
# Date:        16/12/2024
# Company:     xFarm Technologies
################################################################################

import os
from typing import List, Any
import pandas as pd
from copy import deepcopy

from static_color_string import builder_str
from config import TEMPLATE_FOLDER, MODEL_DATA_FOLDER, MODELS_FOLDER

class ModelBuilder():
    def __init__(self, templates_filenames:List[str], simulate:bool):
        self.templates_filenames = templates_filenames
        self.simulate            = simulate

    def get_value(self, param: Any):
        """Get the correct value based on what we are dealing with.

        Args:
            param (Any): Param got from the model data file, single line.

        Returns:
            str: Correctly formatted value.
        """
        # Check if the parameter is a string
        if isinstance(param, str): 
            if not param.startswith("'") and param.endswith("'"):
                param = "'" + param
            elif not param.endswith("'") and param.startswith("'"):
                pass  # Keep the string as it is
            elif not param.startswith("'") and not param.endswith("'"):
                param = "'" + param + "'"
        elif isinstance(param, int):
            param = str(param) + ".0"
        elif isinstance(param, float):
            param = str(param)
        return param
    
    def generate_unique_filename(self, model_name:str):
        """
        Generates a unique filename in a directory following the pattern {model_name}_{progressive_number}.py.
        If a file with the same name exists, increments the progressive number until a unique name is found.
        
        :param directory: Path to the directory where the files are located.
        :param model_name: The base model name to use in the filename.
        :return: A unique filename.
        """
        progressive_number = 0
        while True:
            filename  = f"{model_name}_{progressive_number}.py"
            file_path = os.path.join(MODELS_FOLDER, filename)
            if not os.path.exists(file_path):
                return filename
            progressive_number += 1
    
    def run(self):
        print(f"[{builder_str}] launched.")
        #for each template
        for tf in self.templates_filenames:
            model_data = pd.read_excel(MODEL_DATA_FOLDER + tf + ".xlsx")
            print(f"[{builder_str}] {tf} model will be parameterized.")
            #for each model data
            for md in model_data.to_dict('records'):
                print(f"    ∟ Building: {tf} | {md.get('model_name')}")
                with open(TEMPLATE_FOLDER + tf + '.py', 'r+') as template_f:
                    parameterized_template = ''.join(template_f.readlines())   
                    #for each parameter
                    for param_k, param_v in md.items():
                        parameterized_template = \
                            parameterized_template.replace(f'<{param_k}>', self.get_value(param_v))
                #Dumps model into /results/{model_name}.py
                filename = self.generate_unique_filename(model_name=md.get('model_name'))
                if not self.simulate:
                    with open(MODELS_FOLDER + filename, "w+") as f_model_result:
                        f_model_result.write(parameterized_template)
                    print(f"        ∟ {filename} has been completed and dumped.")
                else:
                    print(f"        ∟ {filename} has been completed and simulated.")
        return
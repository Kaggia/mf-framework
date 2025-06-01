################################################################################
# Module:      tester.py
# Description: Got template and model data, test a new mosaic pipeline file.
# Author:      Stefano Zimmitti
# Date:        16/12/2024
# Company:     xFarm Technologies
################################################################################

import os
import time
import requests
import json
from typing import List
import pandas as pd

from static_color_string import tester_str, success_str, failed_str
from config import MODELS_FOLDER, MODEL_DATA_FOLDER, CONFIG_FOLDER
from utils import get_secret

class ModelTester():
    def __init__(self, templates_filenames:List[str], stage:str, simulate:bool):
        self.templates_filenames = templates_filenames
        self.simulate            = simulate
        self.stage               = stage
    
    def run(self):
        print(f"[{tester_str}] Launched.")
        time.sleep(2)
        #Preparing the dataset to get all testable models.
        models = pd.DataFrame(data={'model_name':[], 'tags':[]})
        for tf in self.templates_filenames:
            model_data = pd.read_excel(MODEL_DATA_FOLDER + tf + ".xlsx")
            model_data = model_data[['model_name', 'tags']]
            models     = pd.concat([models, model_data])

        # Carica il mapping dei criteri
        with open(CONFIG_FOLDER + 'criterion_mappings.json', 'r') as f:
            criterion_mappings = json.load(f)
        
        # Iterate through models DataFrame instead of files
        for index, row in models.iterrows():
            model_name = row['model_name']
            print(f"    ∟ Testing: {model_name}")

            # Extract tags directly from the row
            tags_str = row['tags']
            tags_list = [tag.strip().replace("'", "") for tag in tags_str.strip("[]").split(",")]
            tags_list.sort()
            
            # Find matching criterion
            criterion_chosen = ""
            for criterion, mapping in criterion_mappings.items():
                mapping_tags = sorted(mapping.get('tags', []))
                if mapping_tags == tags_list:
                    criterion_chosen = criterion
                    break

            # Call API to test model
            url = f"https://disease.agrord.xfarm.ag/{self.stage}/api/private/v1/risk_estimate/model/{model_name}?model_type=mosaic_model&filter_by=get_preset({criterion_chosen})"
            api_key = get_secret(secret_name="disease-api-keys", format="json").get(self.stage, None)
            print(f"        ∟ url: {url}")
            
            result = requests.post(
                url=url,
                json={
                    "prevision_day": "2024-12-27T00:00+00:00",
                    "days": 5,
                    "zone_id": "",
                    "measures": [],
                    "persistent": False,
                    "apply_windowing": False,
                    "latitude": 42.321289,
                    "longitude": 12.301677
                },
                headers={'x-api-key': api_key}
            )
            
            status_code = result.status_code
            res = json.loads(result.text)
            print(f"        ∟ Result: {success_str if status_code==200 else failed_str}")
            print(f"        ∟ Message: {'Ok' if status_code==200 else res}")

################################################################################
# Module:      deployer.py
# Description: Got template and model data, deploy a new mosaic pipeline file.
# Author:      Stefano Zimmitti
# Date:        16/12/2024
# Company:     xFarm Technologies
################################################################################

import os
import json
import requests
from colors import colors
from typing import List
import time

from static_color_string import deployer_str, success_str, failed_str, skipped_str
from config import RESULTS_FOLDER, MODELS_FOLDER
from utils import upload_file_to_s3, get_secret

class ModelDeployer():
    def __init__(self, templates_filenames:List[str], stage:str, simulate:bool):
        self.templates_filenames = templates_filenames
        self.simulate            = simulate
        self.stage               = stage
    
    def get_tags_from_raw_model(self, model_file_path:str):
        tags = None
        with open(model_file_path, "r+") as model_file_py:
            model_file_data_raw = model_file_py.readlines()
            for l in model_file_data_raw:
                if 'tags' in l:
                    tags = l.replace(")", "")\
                        .replace("[", "")\
                        .replace("]", "")\
                        .replace("'", "")\
                        .replace("tags=", "")\
                        .replace("\n", "")\
                        .replace(" ", "")
                    tags = tags.split(",")
                    break
        if tags == None:
            raise ValueError("tags have not been properly populated.")
        return tags

    def is_model_already_deployed(self, model_name:str, tags:List[str]):
        tags_base = "tags%3D%5B"
        
        conc_tags = tags_base+''.join([f"+%27{t}%27," for t in tags])
        conc_tags = conc_tags[:-1] + "%5D"
        conc_tags = conc_tags.replace("tags%3D%5B+", "tags%3D%5B")
        
        url         = f"https://mosaic-service.agrord.xfarm.ag/{self.stage}/api/private/v1/get_mosaic_pipeline/model_name/{model_name}?filter_by={conc_tags}"
        api_key     = get_secret(secret_name="mosaic-service-api-keys", format="json").get(self.stage, None)
        result      : requests.Response = \
                    requests.get(
                        url=url,
                        headers={'x-api-key': api_key})
        status_code : int  = result.status_code
        res         : dict = json.loads(result.text)

        return {'status_code':status_code, 'data': res}

    def run(self):
        print(f"[{deployer_str}] Launched.")
        sleep_v = 5
        for i, file_name in enumerate(os.listdir(MODELS_FOLDER)):
            time.sleep(sleep_v)
            print(f"    ∟ Deploying: {file_name}")
            # Only process files matching the pattern {model_name}_{progressive}.py
            if file_name.endswith(".py") and "_" in file_name:
                # Get the full local file path
                file_path     = os.path.join(MODELS_FOLDER, file_name)

                # Split the file name and remove the progressive
                model_name, _ = file_name.rsplit("_", 1)  # Splits by the last underscore
                tags          = self.get_tags_from_raw_model(model_file_path=file_path)

                already_deployed = self.is_model_already_deployed(model_name=model_name, tags=tags)

                if not already_deployed.get('status_code', None)==200:
                    deployed = False
                    while not deployed:
                        # Use the existing function to upload the file
                        r = upload_file_to_s3(
                            file_path=file_path, 
                            bucket_name=f'mosaic-api-service-data-s3-bucket-{self.stage}', 
                            obj_file_name='models/'+f"{model_name}.py", 
                            region_name='eu-west-1')
                        deployed = r.get('result')
                    print(f"        ∟ Deploy: {success_str if r.get('result') else failed_str} | {r.get('message')} | {round((i+1)/len(os.listdir(MODELS_FOLDER))*100, 2)}%")
                    
                    sleep_v = 10
                else:
                    print(f"        ∟ Deploy: {skipped_str} | Already inplace as: {colors.bg.green}{already_deployed.get('data').get('data')}{colors.end} | {round((i+1)/len(os.listdir(MODELS_FOLDER))*100, 2)}%")
                    sleep_v = 0.1
        return
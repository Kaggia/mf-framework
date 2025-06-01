################################################################################
# Module:      main.py
# Description: Deploy macro models into S3 Bucket.
# Author:      Stefano Zimmitti
# Date:        16/12/2024
# Company:     xFarm Technologies
################################################################################

import json
import os
import time

from static_color_string import *
from config import *
from argparse_manager import ArgParser, Option
from builder import ModelBuilder
from deployer import ModelDeployer
from tester import ModelTester
from utils import remove_files_in_dir

if __name__ == "__main__":
    start_time = time.time()
    #--file     [*    | '<TEMPLATE_FILENAME>.py']
    #--build    [True | False] // get the parameters from /model_data/<TEMPLATE_FILENAME>.xlsx
    #--deploy   [True | False] // for each model, deploy to the S3 Bucket selected.
    #--test     [True | False] // for each model, choose wether to launch a test or not.
    #--simulate [True | False] // choose wether or not interact with AWS stack, or hard-weight operations.
    #--stage    [True | False] // select the stage where to deploy to.

    ap = ArgParser(options=[
        Option(n='model',           t=str,   d='all'),
        Option(n='build',           t=bool,  d=False),
        Option(n='deploy',          t=bool,  d=False),
        Option(n='test',            t=bool,  d=False),
        Option(n='simulate',        t=bool,  d=False),
        Option(n='stage',           t=str,   d='test'),
        Option(n='clear',           t=bool,  d=True)])
    parsed_args = ap.get_parsed_args()

    #Clearing already present data in /results
    if parsed_args.get('clear', None)==True:
        print(f"[{main_str}] Removing old data in /results folder.")
        remove_files_in_dir(folder_path=MODELS_FOLDER)

    #Loading the macromodels
    macro_models   = list()
    if not parsed_args.get('model', None)== 'all':
        #Load just the expressed macro_model.py
        macro_models.append(parsed_args.get('model', None))
    else:
        #Load each one of the macro models present in the folder
        for f_name in os.listdir(TEMPLATE_FOLDER):
            macro_models.append(f_name.replace('.py', ''))

    simulation_flag = parsed_args.get('simulate', None)
    
    print(f"[{main_str}] Simulation for this run is {active_str if simulation_flag else skipped_str}")
    print(f"[{main_str}] Working on {json.dumps(macro_models, indent=4)}")

    #Building entry point
    if parsed_args.get('build', None)==True:
        model_builder = ModelBuilder(templates_filenames=macro_models, simulate=simulation_flag)
        model_builder.run()
    else:
        print(f"[{main_str}] Model building process has been {skipped_str}")
    
    #Deploy entry point
    if parsed_args.get('deploy', None)==True:
        model_deployer = ModelDeployer(templates_filenames=macro_models, stage=parsed_args.get('stage', None),simulate=simulation_flag)
        model_deployer.run()
    else:
        print(f"[{main_str}] Model deploy process has been {skipped_str}")

    #Test entry point
    if parsed_args.get('test', None)==True:
        model_tester   = ModelTester(templates_filenames=macro_models, stage=parsed_args.get('stage', None), simulate=simulation_flag)
        model_tester.run()
    else:
        print(f"[{main_str}] Model test process has been {skipped_str}")

    end_time   = time.time()



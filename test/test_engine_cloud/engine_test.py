from mosaic_framework.engine.mosaic_engine import MosaicEngine

import os
import tempfile
import shutil

def build_mosaic_framework():
    os.chdir("../../build_and_deploy")
    
    os.system("sudo python3 main.py")

    os.chdir("../test/test_engine_cloud")
    return 

if __name__== "__main__":
    build_mosaic_framework()

    tmp_folder = tempfile.TemporaryDirectory()

    #Simulate we are in a cloud environment (AWS Lambda)
    #where a temp folder is created, and data preloaded.
    os.mkdir(tmp_folder.name+"/"+"data")
    os.mkdir(tmp_folder.name+"/"+"models")
    os.mkdir(tmp_folder.name+"/"+"results")
    shutil.copy2("data/data.json", tmp_folder.name+"/"+"data"+"/"+"data.json")
    shutil.copy2("models/model.py", tmp_folder.name+"/"+"models"+"/"+"model.py")

    print("------ TEMP FOLDER CONTENT (Post-calculus) ------")
    for root, dirs, files in os.walk(tmp_folder.name):
        level = root.replace(tmp_folder.name, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{sub_indent}{f}')
    print("------                                     ------")


    #Run Engine
    engine = MosaicEngine(
        input_file="model.py", 
        cloud_temp_folder=tmp_folder.name, 
        DEBUG=False)
    engine.run()
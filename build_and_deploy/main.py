################################################################################
# Module:      build_and_deploy.py
# Description: Given a certain module, parse it then create objects.
# Author:      Stefano Zimmitti
# Date:        13/05/2024
# Company:     xFarm Technologies
################################################################################

import os
import time
from datetime import datetime

import colors
from argparse_manager import ArgParser, Option

#Get current version from toml file
def get_current_version(toml_path:str):
    current_version = None
    f    = open(toml_path, "r+")
    data = f.readlines() 
    for line in data:
        c_line = line.replace(' ', '')
        if 'version=' in c_line:
            current_version = c_line\
                .replace(" ", "")\
                .replace('"', '')\
                .replace("\n", '')
            current_version = current_version[current_version.find("=")+1:]
            break
    f.close()
    if current_version == None:
        raise Exception("Version not found in toml file")
    return current_version

#calculate new version, splitting into pieces than incrementing
#accordingly with parameters
def get_new_version(version:str, flags:dict):
    new_version = version
    if not flags['is_debug_version']:
        version = version[:version.rfind('.')]
        splitted_version = [int(value) for value in version.split('.')]
        if flags['is_version_updated']    : 
            splitted_version[0] += 1
            splitted_version[1] = 0
            splitted_version[2] = 0
        elif flags['is_feature_updated']: 
            splitted_version[1] += 1
            splitted_version[2] = 0
        elif flags['is_progressive_updated'] : 
            splitted_version[2] += 1

        date     = datetime.strftime(datetime.now(), "%Y%m%d")
        new_version = ".".join([str(s) for s in splitted_version]) +"."+date

    return version if flags['is_debug_version'] else new_version

#set new version into toml file
def set_new_version(toml_path:str, new_version:str):
    f      = open(toml_path, "r+")
    data = f.readlines()
    for i, line in enumerate(data):
        if 'version =' in line:
            data[i] = f'version = "{new_version}"\n'
            break
    f.close()
    
    #remove old file
    os.remove(toml_path)

    #open a new file with old name, write and close
    f_updt = open("../pyproject.toml", "w+")
    f_updt.writelines(data)
    f_updt.close()
    return

#Removing dependancies, jsut for debug sake.
def set_toml_for_debug():
    toml_config = []
    #get dependancies 
    with open('../pyproject.toml', 'r+') as file:
        toml_config = file.readlines()
        toml_config_for_debug = []
        i_dep = 0
        #find dependencies in toml
        for i, line in enumerate(toml_config):
            if 'dependencies =' in line:
                i_dep = i
                break
            else:
                toml_config_for_debug.append(line)
        toml_config_for_debug.append("dependencies = [\n]\n")
        #find closed bracket dependencies in toml
        for i in range(i_dep+1, len(toml_config)):
            if ']' in toml_config[i]:
                i_closed_dep = i
                break
        #append the rest of the toml
        for i in range(i_closed_dep+1, len(toml_config)):
            toml_config_for_debug.append(toml_config[i])
    with open('../pyproject.toml', 'w+') as file:
        file.writelines(toml_config_for_debug)
    
    os.system("sudo chmod 777 ../pyproject.toml")
    return

#Rollbacking toml file to original and full version of it.
def rollback_toml(file_path="rollback.toml"):
    os.remove("pyproject.toml")
    os.rename(src=file_path, dst="pyproject.toml")
    os.system("sudo chmod 777 pyproject.toml")
    print(f"\n{colors.colors.fg.green}Toml file has been rolled back.{colors.colors.end}")
    return

#Get Gz and whl files versioned, just to upload the correct version.
#and not all of them
def get_versioned_dist_files(version=""):
    gz_file  = f"mosaic_framework-{version}.tar.gz"
    whl_file = f"mosaic_framework-{version}-py3-none-any.whl"
    return gz_file, whl_file

if __name__ == "__main__":
    #Read pyproject.toml
    #Look for version token
    #split into mainVersion.mainFeature.progressiveId.ifBetaOrAlpha
    start_time = time.time()
    ap = ArgParser(options=[
        Option(n='version',         t=bool,  d=False),
        Option(n='feature',         t=bool,  d=False),
        Option(n='progressive',     t=bool,  d=False),
        Option(n='debug',           t=bool,  d=True),
        Option(n='distribute',      t=bool,  d=False)])
    parsed_args = ap.get_parsed_args()

    #Forcing to False debug option, when releasing a new version
    temp_debug_flag = parsed_args['debug']
    parsed_args['debug'] = False \
        if any([parsed_args['version'], parsed_args['feature'], parsed_args['progressive']]) \
        else True
    
    parsed_args['debug'] = all([temp_debug_flag, parsed_args['debug']]) 

    version     = get_current_version(toml_path="../pyproject.toml")
    new_version = get_new_version(
        version=version,
        flags={
            'is_version_updated'       : parsed_args['version'],
            'is_feature_updated'       : parsed_args['feature'],
            'is_progressive_updated'   : parsed_args['progressive'],
            'is_debug_version'         : parsed_args['debug']
        }) 

    print(f"Current version: {colors.colors.fg.yellow}'{version}'{colors.colors.end}")
    print(f"New version    : {colors.colors.fg.green}'{new_version}'{colors.colors.end}")

    #Setting new version of toml
    set_new_version(toml_path="../pyproject.toml", new_version=new_version)
    os.system("sudo chmod 777 ../pyproject.toml")
    
    #Getting a rollback version
    os.system("sudo cp ../pyproject.toml ../rollback.toml")

    print(f"\n{colors.colors.fg.green}pyproject.toml has been updated with new version in it.{colors.colors.end}")
    
    if parsed_args['debug']==True:
        print(f"\n{colors.colors.fg.yellow}Debug mode has been enabled.{colors.colors.end}")
        try:
            set_toml_for_debug()
        except Exception as e:
            print(f"{colors.colors.fg.red}{e}{colors.colors.end} | Trying to update TOML.")
    else:
        print(f"\n{colors.colors.fg.yellow}Debug mode has been {colors.colors.fg.red}not{colors.colors.end} enable.{colors.colors.end}")

    os.chdir("..")

    #Building library
    os.system("sudo python3 -m build")
    print(f"\n{colors.colors.fg.green}Library has been built successfully.{colors.colors.end}")

    #installing library
    os.system(f"sudo pip install --force-reinstall dist/mosaic_framework-{new_version}-py3-none-any.whl")
    print(f"\n{colors.colors.fg.green}Library has been installed successfully.{colors.colors.end}")

    #Deploy part
    #... To AWS Code Artifact Repository
    if parsed_args['distribute']:
        os.system("sudo bash update_aws_codeartifact_token.sh")
        for f in get_versioned_dist_files(version=new_version):
            os.system(f"sudo twine upload --repository codeartifact dist/{f} --verbose")
        print(f"\n{colors.colors.fg.green}Library has been deployed successfully.{colors.colors.end}")
    else:
        print(f"\n{colors.colors.fg.yellow}Library deploying has been skipped.{colors.colors.end}")

    #Rollbacking to original toml file
    if parsed_args['debug']:
        print(f"\n{colors.colors.fg.yellow}Rollbacking to original toml file.{colors.colors.end}")
        rollback_toml()
    
    os.chdir("build_and_deploy")    
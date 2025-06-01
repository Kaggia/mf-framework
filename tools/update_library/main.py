import os
import pkg_resources

import colors
from argparse_manager import ArgParser, Option
from exceptions import PipConfigurationContentException

def print_title():
    print(f"""{colors.colors.fg.cyan}
  __  __                     _        _____                                                    _     
 |  \/  |  ___   ___   __ _ (_)  ___ |  ___|_ __  __ _  _ __ ___    ___ __      __ ___   _ __ | | __ 
 | |\/| | / _ \ / __| / _` || | / __|| |_  | '__|/ _` || '_ ` _ \  / _ \\ \ /\ / // _ \ | '__|| |/ / 
 | |  | || (_) |\__ \| (_| || || (__ |  _| | |  | (_| || | | | | ||  __/ \ V  V /| (_) || |   |   <  
 |_|  |_| \___/ |___/ \__,_||_| \___||_|   |_|   \__,_||_| |_| |_| \___|  \_/\_/  \___/ |_|   |_|\_\                                                                                              
    {colors.colors.end}""")
    print("""
  _                     _                       _                                           _         _               
 | |  ___    ___  __ _ | |  _ __    __ _   ___ | | __ __ _   __ _   ___   _   _  _ __    __| |  __ _ | |_  ___  _ __  
 | | / _ \  / __|/ _` || | | '_ \  / _` | / __|| |/ // _` | / _` | / _ \ | | | || '_ \  / _` | / _` || __|/ _ \| '__| 
 | || (_) || (__| (_| || | | |_) || (_| || (__ |   <| (_| || (_| ||  __/ | |_| || |_) || (_| || (_| || |_|  __/| |    
 |_| \___/  \___|\__,_||_| | .__/  \__,_| \___||_|\_\\__,_| \__, | \___|  \__,_|| .__/  \__,_| \__,_| \__|\___||_|    
                           |_|                              |___/               |_|                                   
""")
    print("""
         _     ___  
 __   __/ |   / _ \ 
 \ \ / /| |  | | | |
  \ V / | | _| |_| |
   \_/  |_|(_)\___/ 
                    
""")
    
def create_pip_config():
    #create a file in ~/.config/pip/
    # Definisci il percorso del file
    config_dir  = os.path.expanduser("~/.config/pip")
    config_file = os.path.join(config_dir, "pip.conf")

    # Crea la directory se non esiste
    os.makedirs(config_dir, exist_ok=True)
    
    #remove if already exists
    if os.path.exists(config_file):
        os.remove(config_file)
        print(f"[{colors.colors.fg.yellow}INFO{colors.colors.end}] Old version of {config_file} removed.")
    content = """
    [global]
    index-url = https://pypi.org/simple
    """
    # Crea il file se non esiste
    with open(config_file, 'w') as f:
        f.write(content.strip())
    # Assegna i permessi 777 al file
    os.chmod(config_file, 0o777)
    print(f"[{colors.colors.fg.yellow}INFO{colors.colors.end}] File {config_file} created e permissions assigned to it.")

def update_pip_config():
    #update pip.conf file
    config_file = os.path.join(os.path.expanduser("~/.config/pip/pip.conf"))
    
    data = None
    with open(config_file, 'r+') as f:
        data = f.readlines()
    
    if data!=None:
        for i, l in enumerate(data):
            if l.startswith("index-url"):
                if i+1 != len(data):
                    data[i+1] = "extra-index-url = https://pypi.org/simple/"
                data.append("\n")
                break
        with open(config_file, 'w+') as f:
            f.writelines(data)

        print(f"\n[{colors.colors.fg.yellow}INFO{colors.colors.end}] pip.conf content updated...")

    else:
        raise PipConfigurationContentException("Cannot properly read pip.conf")
    
    #Checking if good or not
    content_to_check = {'index-url': False, 'extra-index-url': False}
    with open(config_file, 'r+') as f:
        new_content = f.readlines()
        for nc in new_content:
            for c in list(content_to_check.keys()):
                if c in nc:
                    content_to_check[c] = True
    
    if not all([ctc_v for ctc_v in content_to_check.values()]):
        raise PipConfigurationContentException("pip.conf has not been properly updated.")
    else:
        print(f"\n[{colors.colors.fg.green}SUCCESS{colors.colors.end}] pip.conf content checked successfully.")
    return

def is_already_installed():
    installed_packages = pkg_resources.working_set

    # Stampa il nome e la versione di ogni pacchetto
    for package in installed_packages:
        if package.project_name == "mosaic-framework": 
            return True
    return False

if __name__ == "__main__":
    print_title()

    ap = ArgParser(options=[
        Option(n='version', t=str,  d='')])
    parsed_args = ap.get_parsed_args()

    print(f"\n[{colors.colors.fg.yellow}INFO{colors.colors.end}] Chosen version: {colors.colors.fg.blue}{'LATEST' if parsed_args['version']=='' else parsed_args['version']}{colors.colors.end}.")

    is_lib_on_place = is_already_installed()
    print(f"\n[{colors.colors.fg.yellow}INFO{colors.colors.end}] Library mosaic-framework {colors.colors.fg.blue}{'already installed' if is_lib_on_place else 'not installed'}{colors.colors.end}")

    #Handling pip file configuration
    create_pip_config()

    #Logging into AWS CodeArtifact
    print(f"\n[{colors.colors.fg.yellow}INFO{colors.colors.end}] Logging in to AWS CodeArtifact...")
    os.system("sudo aws codeartifact login --tool pip --repository agro-repository --domain agro-domain --domain-owner 267684368050 --region eu-west-1")
    print(f"\n[{colors.colors.fg.green}SUCCESS{colors.colors.end}] Logging in to AWS CodeArtifact successfully.")
    
    update_pip_config()

    #Installing MosaicFramework package
    print(f"\n[{colors.colors.fg.yellow}INFO{colors.colors.end}] Installing MosaicFramework package.")
    version = '' if parsed_args['version']=='' else ('=='+parsed_args['version'])
    os.system(f"sudo pip install mosaic-framework{version} {'--force-reinstall' if is_already_installed else ''}")
    print(f"\n[{colors.colors.fg.green}SUCCESS{colors.colors.end}] MosaicFramework package installed successfully.")
    
    create_pip_config()

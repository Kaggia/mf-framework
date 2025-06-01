from mosaic_framework.engine.mosaic_engine import MosaicEngine

import os
import time

def build_mosaic_framework():
    os.chdir("../../build_and_deploy")
    
    os.system("sudo python3 main.py --debug True")

    os.chdir("../test/test_engine_local")
    return 


if __name__== "__main__":
    start_time = time.time()

    build_mosaic_framework()

    #Run Engine
    engine = MosaicEngine(input_file="Damage_drought.py", DEBUG=False)
    engine.run()

    end_time = time.time()

    print("\n\n\nTotal time taken [entire debug]: ", round(end_time - start_time, 2), " seconds\n\n\n")
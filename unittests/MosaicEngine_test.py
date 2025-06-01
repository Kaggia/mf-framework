import inspect
import unittest
import os
import json
import shutil

from mosaic_framework.engine.mosaic_engine import MosaicEngine

class MosaicEngineTest(unittest.TestCase):
    maxDiff = None  

    """
    Testing MosaicEngine:
        test_0: MosaicPipeline complete with:
            -   Parsing:   not active
            -   Colture:   not active
            -   Validator: not active
        test_1: MosaicPipeline complete with:
            -   Parsing:   active
            -   Colture:   not active
            -   Validator: not active
    """

    def setUp(self) -> None:
        try:
            self.data_folder = "unittests/data/MosaicEngine/"
            try:
                os.mkdir("temp")
                os.mkdir("temp/models")
                os.mkdir("temp/data")
                os.mkdir("temp/results")
            except FileExistsError:
                print("Temp folder already exists, continuing.")
            for filepath in os.listdir(self.data_folder):
                if filepath.endswith(".py"):
                    with open(self.data_folder+filepath, "r+") as mosaic_pipeline_f:
                        temp_f = open(f"temp/models/{filepath[filepath.rfind('/')+1:]}", "wb")
                        temp_f.write(mosaic_pipeline_f.read().encode('utf-8'))
                elif filepath.endswith(".json"):
                    with open(self.data_folder+filepath, "r+") as data_f:
                        temp_f = open(f"temp/data/{filepath[filepath.rfind('/')+1:]}", "wb")
                        temp_f.write(data_f.read().encode('utf-8'))
        except Exception as e:
            print(f"Error during setUp function: {e}")
            self.tearDown(setup_error=True)
            return
        return 
    def tearDown(self) -> None:
        os.chdir("..")
        shutil.rmtree("temp")
        return

    def test_0(self):
        #Home folder where data | models are stored for the run-test
        try:
            os.chdir("temp/") #Home folder where data | models are stored for the run-test
        except:
            pass
        #Run Engine
        engine = MosaicEngine(input_file="test_0.py", DEBUG=False)
        engine.run()

        return

    def test_1(self):
        #Home folder where data | models are stored for the run-test
        try:
            os.chdir("temp/") 
        except:
            pass
        #Run Engine
        engine = MosaicEngine(input_file="test_1.py", parsing_params={'doy_start':121.0}, DEBUG=False)
        engine.run()
        
        return

if __name__ == '__main__':
    unittest.main()


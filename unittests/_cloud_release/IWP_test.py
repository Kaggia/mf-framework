import inspect
import unittest
import os
import json
import shutil

from mosaic_framework.engine.mosaic_engine import MosaicEngine

class IWPModelTest(unittest.TestCase):
    maxDiff = None  

    """
        IWPModelTest:
            - test_iwp_model_results_success: Check if results have been produced.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/_cloud_release/data/IWPTest/"
        self.temp_folder = "temp"

        try:
            os.mkdir("temp")
            os.mkdir("temp/models")
            os.mkdir("temp/data")
            os.mkdir("temp/results")
            
            for filepath in os.listdir(self.data_folder):
                print('filepath: ', filepath)
                if filepath.endswith(".py"):
                    with open(self.data_folder+filepath, "r+") as mosaic_pipeline_f:
                        temp_f = open(f"temp/models/model.py", "wb")
                        temp_f.write(mosaic_pipeline_f.read().encode('utf-8'))
                elif filepath.endswith(".json"):
                    with open(self.data_folder+filepath, "r+") as data_f:
                        temp_f = open(f"temp/data/data.json", "wb")
                        temp_f.write(data_f.read().encode('utf-8'))
        except Exception as e:
            print(f"Error during setUp function: {e}")
            self.tearDown(setup_error=True)
            return
        return 
       
    def tearDown(self, setup_error=False) -> None:
        shutil.rmtree(self.temp_folder)
        return

    def test_iwp_model_results_success(self):
        engine = MosaicEngine(input_file="model.py", cloud_temp_folder="temp", DEBUG=False)
        engine.run()

        #   - ['{OUTPUT_NAME}_start_dataset.csv', '{OUTPUT_NAME}_result.csv', '{OUTPUT_NAME}_rules.csv']
    
        result_check = any(['result' in c for c in os.listdir("temp/results")])

        self.assertTrue(result_check)

        return

if __name__ == '__main__':
    unittest.main()


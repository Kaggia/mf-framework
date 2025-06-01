import inspect
import unittest
import os
import json
import shutil

from mosaic_framework.engine.mosaic_engine import MosaicEngine

class BaroidModelTest(unittest.TestCase):
    maxDiff = None  

    """
        BaroidModelTest:
            - test_wherug_model_results_success: Check if results have been produced.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/_local_release/data/BaroidTest/"
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
                        temp_f = open(f"temp/models/{filepath}", "wb")
                        temp_f.write(mosaic_pipeline_f.read().encode('utf-8'))
                elif filepath.endswith(".json"):
                    with open(self.data_folder+filepath, "r+") as data_f:
                        temp_f = open(f"temp/data/{filepath}", "wb")
                        temp_f.write(data_f.read().encode('utf-8'))
            os.chdir("temp")
        except Exception as e:
            print(f"Error during setUp function: {e}")
            self.tearDown(setup_error=True)
            return
        return 
       
    def tearDown(self, setup_error=False) -> None:
        if not setup_error: 
            os.chdir("..")
        shutil.rmtree(self.temp_folder)
        return

    def test_wherug_model_results_success(self):
        engine = MosaicEngine(input_file="baroid_model_local.py", DEBUG=False)
        engine.run()

        #   - ['{OUTPUT_NAME}_start_dataset.csv', '{OUTPUT_NAME}_result.csv', '{OUTPUT_NAME}_rules.csv']
        start_dataset_check = any(['start_dataset' in c for c in os.listdir("results")])
        result_check        = any(['result'        in c for c in os.listdir("results")])
        rules_check         = any(['rules'         in c for c in os.listdir("results")])

        self.assertTrue(start_dataset_check)
        self.assertTrue(result_check)
        self.assertTrue(rules_check)
        return

if __name__ == '__main__':
    unittest.main()


import os
import json
import inspect
import pandas as pd
import unittest
import shutil

from mosaic_framework.data_storage.data_storage import MosaicDataStorage
from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
from mosaic_framework.model.model import Model
from mosaic_framework.components.exceptions import ComponentParameterException
from mosaic_framework.core.comparative_factors import *
from mosaic_framework.core.math_factors import *
from mosaic_framework.core.output_factors import *
from mosaic_framework.core.growth_models_factors import *

class TestModel(unittest.TestCase):
    """
    Testing Model:
        test_model_0: testing the Model Component.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/Model/"
        try:
            os.mkdir("temp")
            os.mkdir("temp/data")
        except FileExistsError:
            pass
        shutil.copyfile(self.data_folder + f"input_data.json", 'temp/data/data.json')
        os.chdir("temp/")
        return 
    def tearDown(self) -> None:
        os.chdir("..")

        shutil.rmtree("temp")

        return

    def test_model_granularity_presence_success(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)

        m = Model(
            label="agro_model", 
            outputs=['infection'],
            granularity='daily',
            history=3,
            infection=[
                DayOfYear(column='doy', target='sampleDate'),
                AndComparativeAgroRule(
                    column='factor_start', 
                    rules=[
                        SimpleComparativeRule(target='doy', condition='goet121.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='doy', condition='loet243.0', is_implicit=True, debug=True)]),
                AndComparativeAgroRule(
                    column='factor_germ', 
                    rules=[
                        SimpleComparativeRule(target='avgTemp', condition='goet15.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='avgTemp', condition='loet35.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='avgHumidity', condition='goet70.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True)
                        ]),
                ApplyFunctionOnRange(
                    column='factor_germ_fnc',
                    target='factor_germ',
                    range=(2, 0),
                    function=sum),
                AndComparativeAgroRule(
                    column='factor_dis', 
                    rules=[
                        SimpleComparativeRule(target='rain', condition='goet3.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='factor_germ_fnc', condition='goet1.0', is_implicit=True, debug=True)
                    ]),
                ApplyFunctionOnRange(
                    column='factor_dis_fnc',
                    target='factor_dis',
                    range=(2, 0),
                    function=sum),
                AndComparativeAgroRule(
                    column='infection_result', 
                    rules=[
                        SimpleComparativeRule(target='avgTemp', condition='goet18.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='avgTemp', condition='loet32.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='avgHumidity', condition='goet80.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='leafWetness', condition='goet9.0', is_implicit=True, debug=True),
                        SimpleComparativeRule(target='factor_dis_fnc', condition='goet1.0', is_implicit=True, debug=True)
                    ])
            ],
            infection_output_rule=SelectMaxAndCompare(column='infection', target='infection_result', condition='goet1.0', ref=0))
        m.set_storage(data_storage=self.data_storage)
        m.set_memory(shared_memory=self.shared_memory)
        
        self.assertEqual(m.granularity, "daily")
        return

    def test_model_granularity_absence_failure(self):
        self.data_storage   = MosaicDataStorage(DEBUG=True)
        self.data_storage.allocate()
        self.shared_memory  = MosaicSharedMemory(DEBUG=True)

        #Missing granularity
        with self.assertRaises(ComponentParameterException):
            m = Model(
                label="agro_model", 
                infections=['infection'],
                history=3,
                infection=[
                    DayOfYear(column='doy', target='sampleDate'),
                    AndComparativeAgroRule(
                        column='factor_start', 
                        rules=[
                            SimpleComparativeRule(target='doy', condition='goet121.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='doy', condition='loet243.0', is_implicit=True, debug=True)]),
                    AndComparativeAgroRule(
                        column='factor_germ', 
                        rules=[
                            SimpleComparativeRule(target='avgTemp', condition='goet15.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='avgTemp', condition='loet35.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='avgHumidity', condition='goet70.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True)
                            ]),
                    ApplyFunctionOnRange(
                        column='factor_germ_fnc',
                        target='factor_germ',
                        range=(2, 0),
                        function=sum),
                    AndComparativeAgroRule(
                        column='factor_dis', 
                        rules=[
                            SimpleComparativeRule(target='rain', condition='goet3.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='factor_germ_fnc', condition='goet1.0', is_implicit=True, debug=True)
                        ]),
                    ApplyFunctionOnRange(
                        column='factor_dis_fnc',
                        target='factor_dis',
                        range=(2, 0),
                        function=sum),
                    AndComparativeAgroRule(
                        column='infection_result', 
                        rules=[
                            SimpleComparativeRule(target='avgTemp', condition='goet18.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='avgTemp', condition='loet32.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='avgHumidity', condition='goet80.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='leafWetness', condition='goet9.0', is_implicit=True, debug=True),
                            SimpleComparativeRule(target='factor_dis_fnc', condition='goet1.0', is_implicit=True, debug=True)
                        ])
                ],
                infection_infection_rule=SelectMaxAndCompare(column='infection', target='infection_result', condition='goet1.0', ref=0))
            m.set_storage(data_storage=self.data_storage)
            m.set_memory(shared_memory=self.shared_memory)
        
        return


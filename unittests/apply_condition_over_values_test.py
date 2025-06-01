import json
import inspect
import pandas as pd
import unittest

from mosaic_framework.core.functions import apply_condition_over_values

class ApplyConditionOverValues(unittest.TestCase):
    """
    Testing SelectMaxAndCompare:
        test_0: Base case.
        test_1: Base case with a list of target containing one only.
    """

    def setUp(self) -> None:
        self.data_folder = "unittests/data/SelectMaxAndCompare/"
        return 
    
    def tearDown(self) -> None:
        return

    def test_0(self):
        cond = {'comp': 'goet', 'val': 1.0}
        lista= [2,2,2,2]
        res = apply_condition_over_values(v= lista, cond=cond,iterable_fnc=all)
        self.assertEqual(1, res)
        return

    def test_1(self):
        cond = {'comp': 'goet', 'val': 1.0}
        lista= [1,2,0,2]
        res = apply_condition_over_values(v= lista, cond=cond,iterable_fnc=all)
        print(res)
        self.assertEqual(0, res)
        return

    def test_2(self):
        cond = {'comp': 'goet', 'val': 2.0}
        lista= [0,2,0,2]
        res = apply_condition_over_values(v= lista, cond=cond,iterable_fnc=any)
        print(res)
        self.assertEqual(1, res)
        return

if __name__ == '__main__':
    unittest.main()
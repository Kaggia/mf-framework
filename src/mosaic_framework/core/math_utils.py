################################################################################
# Module: math_utils.py
# Description: Implementation of math utils.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from typing import Callable
from statistics import mean, median
from math import sin, cos, tan
from math import asin as arcsin, acos as arccos, atan as arctan

def get_mapped_function(fnc: str) -> Callable:
    """
    Maps a string function name to its corresponding callable function.

    Args:
        fnc (str): Name of the function to map

    Returns:
        Callable: The mapped function
    """
    mappings = {'sum': sum, 'mean': mean, 'median':median, 
                'avg': mean, 'min': min,  'max': max,
                'sin': sin,  'cos': cos,  'tan': tan,
                'arcsin': arcsin,  'arccos': arccos,  'arctan': arctan}
    return mappings[fnc]

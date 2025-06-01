################################################################################
# Module: functions.py
# Description: Implementation of functions, to be applied on a single row.
# Author:     Stefano Zimmitti
# Date: 15/01/2024
# Company: xFarm Technologies
################################################################################

from typing import Callable, List, Dict

def and_rule_over_row(row: Dict, **kwargs) -> int:
    """
    Apply an AND rule over the row.

    Args:
        row (Dict): Row to be checked
        columns (List): List of columns to be checked

    Returns:
        int: 1 if the condition is met, 0 otherwise
    """
    #needs columns:list
    columns = kwargs.get('columns')
    result = 1
    for col in columns:
        result *= row[col]
    return result

def or_rule_over_row(row: Dict, **kwargs) -> int:
    """
    Apply an OR rule over the row.

    Args:
        row (Dict): Row to be checked
        columns (List): List of columns to be checked

    Returns:
        int: 1 if the condition is met, 0 otherwise
    """
    #needs columns:list
    columns = kwargs.get('columns')
    result = 0
    for col in columns:
        result += row[col]
    return 1 if result>0 else 0

def apply_condition(v: float, cond: Dict) -> int:
    """
    Apply a condition over a value.

    Args:
        v (float): Value to be checked
        cond (Dict): Condition to be applied

    Returns:
        int: 1 if the condition is met, 0 otherwise
    """
    operators = {
            'gt'  :  v >  cond['val'],
            'goet':  v >= cond['val'],
            'lt'  :  v <  cond['val'],
            'loet':  v <= cond['val'],
            'et'  :  v == cond['val']
        }

    return 1 if operators[cond['comp']] else 0

def apply_condition_over_values(v: List, cond: Dict, iterable_fnc: Callable) -> int:
    """
    Apply a condition over a value.

    Args:
        v (List): Values to be checked
        cond (Dict): Condition to be applied
        iterable_fnc (Callable): Function to apply over the list of results

    Returns:
        int: 1 if the condition is met, 0 otherwise
    """
    operators = {
            'gt'  :  iterable_fnc([v_ >  cond['val']  for v_ in v]),
            'goet':  iterable_fnc([v_ >= cond['val']  for v_ in v]),
            'lt'  :  iterable_fnc([v_ <  cond['val']  for v_ in v]),
            'loet':  iterable_fnc([v_ <=  cond['val'] for v_ in v]),
            'et'  :  iterable_fnc([v_ ==  cond['val'] for v_ in v])
    }

    return 1 if operators[cond['comp']] else 0

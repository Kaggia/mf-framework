################################################################################
# Module:      fill_parameter_methods.py
# Description: Functions used to fill certain parameters.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################

import random
import string

def fill_label_source() -> str:    
    """
    Method used to fill the label parameter in the Source object if it is empty.
    ---\n
    params:
    None
    ---\n
    returns: 
    str - A random string made up of five characters.
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(5))

def skip_autofill() -> str:    
    """
    Method used to skip filling.
    ---\n
    params:
    None
    ---\n
    returns: 
    str - Empty string.
    """
    return ''

def get_zero() -> int:
    """
    Method used to get zero.
    ---\n
    params:
    None
    ---\n
    returns: 
    int - Zero.
    """
    return 0
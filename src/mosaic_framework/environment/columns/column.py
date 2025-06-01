################################################################################
# Module:      column.py
# Description: Collection of columns that can be detected or assigned.
# Author:      Stefano Zimmitti
# Date:        26/08/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.components.components import InternalComponent


class Column(InternalComponent):
    """
    Parent class for all columns.

    Args:
        name (str): name of the column.
    """
    def __init__(self, name:str):
        self.name = name
    def to_dict(self):
        return {self.name:self.__class__.__name__}


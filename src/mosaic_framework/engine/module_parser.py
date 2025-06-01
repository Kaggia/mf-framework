################################################################################
# Module:      module_parser.py
# Description: Given a package, find and return the classes in it.
# Author:      Stefano Zimmitti
# Date:        26/08/2024
# Company:     xFarm Technologies
################################################################################

import inspect
from typing import List

class ModuleParser:
    def __init__(self) -> None:
        pass
    """
    ModuleParser class to find and return the classes in a given module.
    """

    def get(self, module, filter_by_parent=None):
        """
        This method is used to get classes from a module.
        It returns a list of classes found in the module.
        If filter_by_parent is provided, it returns only the classes that are subclasses of the provided parent class.
        If filter_by_parent is not provided, it returns all classes in the module.

        Example usage:
        module_parser = ModuleParser()

        Args:
            module (_type_): _description_
            filter_by_parent (_type_, optional): _description_. Defaults to None.

        Returns:
            classes: List of classes found
        """
        classes = []
        if filter_by_parent != None:
            for name, cls in inspect.getmembers(module):
                if inspect.isclass(cls) and issubclass(cls, filter_by_parent) and cls is not filter_by_parent:
                    classes.append(cls)
                    print(cls)
        else:
            for name, cls in inspect.getmembers(module):
                if inspect.isclass(cls):
                    classes.append(cls)
        return classes
    
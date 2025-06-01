################################################################################
# Module:      output_rule_parser.py
# Description: Given a rule as dict, parse it then create object.
# Author:      Stefano Zimmitti
# Date:        19/06/2024
# Company:     xFarm Technologies
################################################################################

import ast
import pkgutil
import importlib
from typing import List


import mosaic_framework
import mosaic_framework.core

import mosaic_framework.core.output_factors
from mosaic_framework.core.output_factors import OutputAgroRule
from mosaic_framework.engine.exceptions import ClassNotFoundException

class OutputRuleParser():
    """
    Main goal of this class is to parse a single OutputRule,
    then try to match any object that is part of mosaic_framework package. If an object
    is found it is added returned, otherwise an error is raised.
    """
    def __init__(self, rules_hub) -> None:
        self.rules_hub = rules_hub

    def get_modules(self)->list:
        """
        Run over mosaic_framework package, find any module that is part of the package
        then add them to a list. Modules are found dynamically at runtime.
        ---\n
        params:
        None
        ---
        returns: List of modules find in mosaic_framework path.
        """
        # Scorri tutti i moduli nel package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.__path__, mosaic_framework.__name__ + '.'):
            # Importa il modulo dinamicamente
            module = importlib.import_module(module_name)
            # Aggiungi il modulo alla lista
            modules.append(module)

        return modules

    def import_and_create_objects(self, modules:List[object], class_call:dict)->list:
        """
        Allow to match the objects found in module (class_calls) with any module
        find in modules. Then it returns a list of objects that match, otherwise
        a ClassNotFoundException is raised.
        ---\n
        params:
        modules:List[object] -> List of modules found in a package 
        class_calls:list -> List of objects found in a module
        ---
        Returns:
        -  List of objects that are found in modules.
        """

        obj_found = False
        #example of class_class:
        # {
        #     "func": "SelectMaxAndCompare",
        #     "args": [],
        #     "kwargs": {
        #         "column": "primary",
        #         "target": "primary_result",
        #         "condition": "goet1.0",
        #         "ref": 0
        #     }
        # }
        class_name = class_call['func']
        args       = class_call['args']
        kwargs     = class_call['kwargs']
        for m in modules:
            if hasattr(m, class_name):
                cls = getattr(m, class_name)
                # Instanciate new object as AST expression
                # Need to cast each <_ast.Constant> to the value in it.
                obj = cls(*args, **kwargs) 
                #print(f'Oggetto creato della classe {class_name} con args: {args} e kwargs: {kwargs}')
                obj_found = True
        if not obj_found:
            raise ClassNotFoundException(f"Class: {class_name} cannot be found in modules available.")
        obj_found = False
        return obj

    def parse(self, output_rule)->OutputAgroRule:
        """
        Entry point of Class, it runs parse_model_file method and import_and_create_objects, 
        returning eventually the objects found in the modules.
        ---\n
        params:
        None
        ---
        Returns:
        -  OutputAgroRule .
        """

        # Import modules e instanciate objects
        obj     = self.import_and_create_objects(
            modules=self.get_modules(), 
            class_call=output_rule)
        
        obj.set_rules_hub(self.rules_hub)
                        
        return obj

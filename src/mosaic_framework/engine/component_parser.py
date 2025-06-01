################################################################################
# Module:      module_parser.py
# Description: Given a certain module, parse it then create objects.
# Author:      Stefano Zimmitti
# Date:        13/05/2024
# Company:     xFarm Technologies
################################################################################
from __future__ import annotations
from typing import List, TYPE_CHECKING

import ast
from typing import List, Tuple, Dict
import importlib
import pkgutil

import mosaic_framework
from mosaic_framework.components.components import Component
from mosaic_framework.engine.exceptions import ClassNotFoundException

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory

class ComponentParser:
    """
    Main goal of this class is to run over a single module.py, find objects declared,
    then try to match any object that is part of mosaic_framework package. If an object
    is found it is added to available objects, otherwise an error is raised
    """
    def __init__(self, DEBUG=False) -> None:
        self.DEBUG        = DEBUG
        self.data_storage = None

    def get_modules(self) -> list:
        """
        Run over mosaic_framework package, find any module that is part of the package
        then add them to a list. If self.DEBUG=True then displays the found modules. 
        Modules are found dynamically at runtime.
        """
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.__path__, mosaic_framework.__name__ + '.'):
            module = importlib.import_module(module_name)
            modules.append(module)

        if self.DEBUG:
            for m in modules:
                print(str(m))

        return modules

    def set_storage(self, data_storage:MosaicDataStorage) -> None:
        self.data_storage = data_storage
    
    def parse_model_file(self, model_content: object) -> List[Tuple[str, List, Dict]]:
        """
        Dynamically parse a Python module using ast Python module. Returning every each
        objects. Objects are returned in a list 
        """
        
        class_calls = []

        tree = ast.parse(model_content)

        for d in ast.walk(tree):
            if isinstance(d, ast.Call):
                if isinstance(d.func, ast.Name):
                    class_name = d.func.id
                    args = [self.parse_internal_kwargs(arg) if isinstance(arg, (ast.Call, ast.List, ast.Dict)) else arg for arg in d.args]
                    kwargs = {kw.arg: self.parse_internal_kwargs(kw.value) for kw in d.keywords}
                    class_calls.append((class_name, args, kwargs))
        return class_calls

    def parse_internal_kwargs(self, internal_kwargs) -> dict:
        """
        Walking through agronomical model module returns a collection (as dictionary)
        of ast objects (Expression, Call, Constant, Keyword, etc.), we need to furtherly 
        parse in order to correctly populate the Component's parameters.
        """
        def visit_and_extract_constants(d):
            if isinstance(d, dict):
                result = {}
                for key, value in d.items():
                    result[key] = visit_and_extract_constants(value)
                return result
            elif isinstance(d, list):
                return [visit_and_extract_constants(item) for item in d]
            elif isinstance(d, ast.Constant):
                return d.value
            elif isinstance(d, ast.List):
                return [visit_and_extract_constants(item) for item in d.elts]
            elif isinstance(d, ast.Call):
                func_name = visit_and_extract_constants(d.func)
                args = [visit_and_extract_constants(arg) for arg in d.args]
                kwargs = {kwarg.arg: visit_and_extract_constants(kwarg.value) for kwarg in d.keywords}
                return {'func': func_name, 'args': args, 'kwargs': kwargs}
            elif isinstance(d, ast.Name):
                return d.id
            elif isinstance(d, ast.Attribute):
                return f"{visit_and_extract_constants(d.value)}.{d.attr}"
            elif isinstance(d, ast.BinOp):
                left = visit_and_extract_constants(d.left)
                right = visit_and_extract_constants(d.right)
                op = type(d.op).__name__
                return {'left': left, 'op': op, 'right': right}
            elif isinstance(d, ast.UnaryOp):  # Aggiungi il supporto per ast.UnaryOp
                operand = visit_and_extract_constants(d.operand)
                op = type(d.op).__name__
                return {'op': op, 'operand': operand}
            elif isinstance(d, ast.keyword):
                return {d.arg: visit_and_extract_constants(d.value)}
            elif isinstance(d, ast.Expr):
                return visit_and_extract_constants(d.value)
            elif isinstance(d, ast.Tuple):
                return tuple(visit_and_extract_constants(item) for item in d.elts)
            else:
                return str(d)

        return visit_and_extract_constants(internal_kwargs)    
    
    def import_and_create_objects(self, modules: List[object], class_calls: list) -> list:
        """
        Allow to match the objects found in module (class_calls) with any module
        find in modules. Then it returns a list of objects that match, otherwise
        a ClassNotFoundException is raised.
        """
        objects = []
        obj_found = False
        for class_name, args, kwargs in class_calls:
            for m in modules:
                if hasattr(m, class_name):
                    cls = getattr(m, class_name)
                    if issubclass(cls, Component):
                        obj = cls(*args, **kwargs)
                        objects.append(obj)
                    obj_found = True
            if not obj_found:
                raise ClassNotFoundException(f"Class: {class_name} cannot be found in modules available.")
            obj_found = False
        return objects

    def parse(self, model_label:str) -> List[Component]:
        """
        Entry point of Class, it runs parse_model_file method and import_and_create_objects, 
        returning eventually the objects found in the modules.
        """
        
        print(f"[Component Parser] model file is retrieved from MosaicDataStorage")
        
        #Model content contains the parsed (eventually) model 
        # (MosaicPipeline) file
        model_content = self.data_storage.get_resource(
            label=model_label, 
            error_policy = 'raise').get_data()

        class_calls = self.parse_model_file(model_content=model_content)
        objects     = self.import_and_create_objects(modules=self.get_modules(), class_calls=class_calls)
        filtered_objects = [o for o in objects if isinstance(o, Component)]
        return filtered_objects

################################################################################
# Module:      rule_parser.py
# Description: Given a certain set of rules as dict, parse it then create objects.
# Author:      Stefano Zimmitti
# Date:        30/05/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import json
from typing import Any
import pkgutil
import importlib
from collections import deque

if TYPE_CHECKING:
    from mosaic_framework.core.environment.rules_hub import MosaicRulesHub
    
    MosaicRulesHubType  = MosaicRulesHub


import mosaic_framework
import mosaic_framework.core

from mosaic_framework.engine.exceptions import ClassNotFoundException

class RuleParser():
    """
    Main goal of this class is to run over a list of rules described as dicts, also nested, 
    find objects declared, then try to match any object that is part of mosaic_framework.core package. 
    If an object is found it is replaced in the structure, otherwise an error is raised. 
    This is a particular case of a Breadth-First Search, where each node is parsed and 
    updated with its 'object' version. 
    """

    def __init__(self, rules_hub:MosaicRulesHubType) -> None:
        self.core_rules_modules = self.get_core_modules()
        self.rules_hub          = rules_hub
    
    def get_core_modules(self)->list:
        """
        Run over mosaic_framework.core package, find any module that is part of the package
        then add them to a list. Modules are found dynamically at runtime.
        ---\n
        params:
        None
        ---
        Returns:
        -  List[module] -> modules find in mosaic_framework path.
        """
        # Scorri tutti i moduli nel package
        modules = list()
        for _loader, module_name, _ispkg in pkgutil.walk_packages(mosaic_framework.core.__path__, mosaic_framework.core.__name__ + '.'):
            # Importa il modulo dinamicamente
            module = importlib.import_module(module_name)
            # Aggiungi il modulo alla lista
            modules.append(module)    
        return modules

    def get_object(self, node, parent, key):
        """
        Run over modules, finding the one that matches. 
        Modules are found dynamically at runtime, but not in this function. 
        They are in: self.core_rules_modules, got from get_core_modules function.
        ---\n
        params:
        None
        ---
        Returns:
        -  node: dict -> visited node.
        """
        # Implement your logic here
        # For now, we just print the node
        obj       = None
        obj_found = False
        for m in self.core_rules_modules:
            if hasattr(m, node['func']):
                cls = getattr(m, node['func'])
                # Instanciate new object as AST expression
                # Need to cast each <_ast.Constant> to the value in it.
                obj = cls(*node['args'], **node['kwargs']) 
                obj.set_rules_hub(self.rules_hub)
                obj_found = True
        if obj_found == False:
            raise ClassNotFoundException(f"Class: {node['func']} cannot be found in modules (cores) available.")
       
        # Replace the node with 1 in the parent
        if parent is not None and key is not None:
            parent[key] = obj
        return node  # Replace with actual object creation logic
                
    def collect_all_nodes_by_levels(self, nodes):
        """ 
        Collects nodes from an initial list, exploring them level by level in a hierarchical structure.
        Returns a list of lists, where each sublist contains nodes at the same level.
        Each tuple in the sublist contains:
        - Current node.
        - Parent node.
        - Key in the parent node pointing to the current node.
        ---\n
        Parameters:
        - nodes (list): List of initial nodes, where each node is a dictionary.
        ---\n
        Returns:
        - list: List[List], representing nodes organized by level.
        """        
        
        levels = []
        queue = deque([(node, 0, None, None) for node in nodes])
        max_level = 0
        
        while queue:
            current, level, parent, key = queue.popleft()
            if level > max_level:
                max_level = level
                levels.append([])
            
            if len(levels) <= level:
                levels.append([])
            levels[level].append((current, parent, key))
            
            for k, value in current.get("kwargs", {}).items():
                if isinstance(value, dict):
                    queue.append((value, level + 1, current["kwargs"], k))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            queue.append((item, level + 1, value, i))
        return levels
    
    def visit(self, nodes):
        """ 
        Visits nodes starting from the lowest level.
        Collects nodes into levels using 'collect_all_nodes_by_levels' method, then visits nodes in reverse order of levels.
        Replaces the root nodes with their corresponding objects after visiting.
        Returns a set of visited node IDs.
        ---\n
        Parameters:
        - nodes (list): List of initial nodes, where each node is a dictionary.
        ---\n
        Returns:
        - set: Set of visited node IDs.
        """
        levels = self.collect_all_nodes_by_levels(nodes)
        visited = set()

        for level_nodes in reversed(levels):
            for node, parent, key in level_nodes:
                self.get_object(node, parent, key)
                visited.add(id(node))
        
        # Replace the root nodes
        for i, node in enumerate(nodes):
            nodes[i] = self.get_object(node, None, None)
            

        return visited        
    
    def visit_root(self, unparsed_root:dict)->list:
        """ 
        Visits the root node of the AST.
        Parses the unparsed root node into an AST expression using core rules modules.
        Returns the parsed root node as a list.
        
        Parameters:
        - unparsed_root (dict): Dictionary representing the unparsed root node, containing 'func', 'args', and 'kwargs'.
        
        Returns:
        - list: Parsed root node as a list.
        """
        for m in self.core_rules_modules:
            if hasattr(m, unparsed_root['func']):
                cls = getattr(m, unparsed_root['func'])
                # Instanciate new object as AST expression
                # Need to cast each <_ast.Constant> to the value in it.
                parsed_root = cls(*unparsed_root['args'], **unparsed_root['kwargs']) 
                break
        return parsed_root

    def parse(self, output_rules:dict):
        """ 
        Entry point of the class. Running over an entire dict, replacing each rule with 
        the corrispective object found un core module.
        
        Parameters:
        - output_rules (dict): Dictionary representing the unparsed root node, containing 'func', 'args', and 'kwargs'.
        
        Returns:
        - output_rules: Parsed root node as a list.
        """
        
        #(Breadth-First Search, BFS)
        self.visit(output_rules)

        for i, n in enumerate(output_rules):
            output_rules[i] = self.visit_root(output_rules[i])
            output_rules[i].set_rules_hub(self.rules_hub)

        return output_rules
    

################################################################################
# Module:      detect_engine.py
# Description: Collection of engine needed to 'auto' detect columns.
# Author:      Stefano Zimmitti
# Date:        26/08/2024
# Company:     xFarm Technologies
################################################################################

import re
from typing import Protocol, List
import pandas as pd
import json

from mosaic_framework.environment.exceptions import DuplicateMappingColumnsException

class ProtocolColumnDetectEngine(Protocol):    
    def run(self):
        ...

class LevenshteinDistanceColumnDetectEngine(ProtocolColumnDetectEngine):
    """
    Implementation of the Levenshtein algorithm that calculates the distance
    between two strings. This class is used to find the best match between a column

    Args:
        duplicate_policy (str): It allows to chose the policy about the duplicates.
    """
    def __init__(self, duplicate_policy:str):
        self.duplicate_policy = duplicate_policy
        self.threshold        = 0.95

    @staticmethod
    def clean_string(s):
        """
        clean a string from underscore and convert it to lowercase
        """
        # Funzione per normalizzare i nomi rimuovendo underscore e convertendo in minuscolo
        return re.sub(r'[_]', '', s).lower()
    
    @staticmethod
    def levenshtein_distance(str1, str2):
        """
        Calculates the Levenshtein distance between two strings.
        """
        # Inizializza la matrice di distanza
        len_str1 = len(str1) + 1
        len_str2 = len(str2) + 1

        # Crea una matrice vuota
        dp = [[0 for _ in range(len_str2)] for _ in range(len_str1)]

        # Inizializza i valori della prima riga e della prima colonna
        for i in range(len_str1):
            dp[i][0] = i
        for j in range(len_str2):
            dp[0][j] = j

        # Calcola la distanza di Levenshtein
        for i in range(1, len_str1):
            for j in range(1, len_str2):
                cost = 0 if str1[i-1] == str2[j-1] else 1
                dp[i][j] = min(dp[i-1][j] + 1,        # Cancellazione
                            dp[i][j-1] + 1,        # Inserzione
                            dp[i-1][j-1] + cost)   # Sostituzione

        # La distanza finale si trova in dp[len_str1-1][len_str2-1]
        return dp[-1][-1]
    
    @staticmethod
    def validate(mapping:dict, duplicate_policy:str):
        """
            Validate mapping got from the algorithm, based on what policy is specified,
            then apply a different logics.

            Args:
                mapping: dict, mapping of the columns to the classes
                duplicate_policy: str, policy to apply in case of duplicate columns

        """

        def remove_duplicates(in_mapping:dict):
            """
            Remove duplicates from the mapping dictionary.
            """
            # Remove duplicates based on the 'class' key
            unique_values = set()
            unique_mapping = {}
            ordered_mapping  = dict(sorted(in_mapping.items(), key=lambda item: item[1]["distance"]))

            for key, value in ordered_mapping.items():
                if value['class'] not in unique_values:
                    unique_values.add(value['class'])
                    unique_mapping[key] = value

            return unique_mapping
        
        check_unicity = len([v['class'] for k,v in mapping.items()]) - len(set([v['class'] for k,v in mapping.items()])) > 0
        if check_unicity and duplicate_policy == 'raise':
            raise DuplicateMappingColumnsException('Duplicate columns detected')
        elif check_unicity and duplicate_policy == 'best':
            mapping = remove_duplicates(in_mapping=mapping)
        return mapping
    
    @staticmethod
    def get_classes_list(classes:List[str]):
        """
        Each class <Column> has a set of other_names, so if the obj created from 
        that class has 'other_names'.
        """
        upper_classes = [str(c).split('.')[-1].replace("'>", "") for c in classes]
        for c in classes:
            obj_col = c(name='NONE')
            if obj_col.__dict__.get('other_names'):
                upper_classes.extend(obj_col.other_names)
        upper_classes = list(set(upper_classes))
        
        return upper_classes
    
    @staticmethod
    def map_names_to_classes(preremap_columns:dict, classes:List[str]):
        """
        Map the columns to the classes.
        """
        result = preremap_columns.copy()
        for column_name in result.keys():
            for c in classes:
                obj_col = c(name='NONE')
                if obj_col.__dict__.get('other_names'):
                    if result[column_name]['class'] in obj_col.other_names or result[column_name]['class']==str(c).split('.')[-1].replace("'>", ""):
                        result[column_name]['class'] = str(c).split('.')[-1].replace("'>", "")
        return result

    def find_best_match(self, column_name, class_names):
        """
        This function finds the best match between a column name and a list of class names
        """
        # Normalizza il nome della colonna
        normalized_col = self.clean_string(column_name)
        
        # Calcola la distanza di Levenshtein tra il nome della colonna e ogni nome di classe
        best_match = None
        lowest_distance = float('inf')
        
        for class_name in class_names:
            normalized_class = self.clean_string(class_name)
            distance = self.levenshtein_distance(normalized_col, normalized_class)
            
            if distance < lowest_distance:
                lowest_distance = distance
                best_match = class_name
                
        return best_match, lowest_distance

    def run(self, classes:List, data_columns:List[str]):
        """
        Run the Levenshtein Distance algorithm to detect columns.
        """
        #clean the modules names, extracting just the class name.
        print(f"[LevenshteinDistanceColumnDetectEngine] {classes}")

        #This List is made up by classes name and 'other_names' attribute content
        #of each class that has 'other_names' in its set of variables.
        #What does that mean? That if we are going to map an element of this set of 
        #names, we need to re-map again on the class name.
        columns_as_classes = self.get_classes_list(classes=classes)

        # Mappatura delle colonne alle classi
        detailed_column_class_mapping = {}

        for column in data_columns:
            best_match_class, distance = self.find_best_match(column, columns_as_classes)
            detailed_column_class_mapping[column] = {'class': best_match_class, 'distance': distance}

        #Remapping over Class' name
        detailed_column_class_mapping = self.map_names_to_classes(preremap_columns=detailed_column_class_mapping, classes=classes)

        #Holding a copy of results
        backup_column_class_mapping = detailed_column_class_mapping.copy()

        detailed_column_class_mapping = self.validate(mapping=detailed_column_class_mapping, duplicate_policy=self.duplicate_policy)

        #Check wether the number of classes is greater than the threshold, if so, assign generic column class
        #This is needed to handles validation case, where a lot of columns are "dumped" cause
        #they fall on the same column, because they do not have the right Column reference.
        column_class_mapping = {}
        print(f"[LevenshteinDistanceColumnDetectEngine] Columns before validation: {len(backup_column_class_mapping)} | Columns after validation: {len(list(detailed_column_class_mapping.keys()))}")        
        print(f"[LevenshteinDistanceColumnDetectEngine] Columns before validation: {json.dumps(backup_column_class_mapping, indent=4)}")        

        cond1 = len(detailed_column_class_mapping)<float(len(backup_column_class_mapping) * self.threshold)
        cond2 = not len(detailed_column_class_mapping)==len(backup_column_class_mapping)
        cond3 = not len(detailed_column_class_mapping)==int(len(backup_column_class_mapping) * self.threshold)
        print(f"[LevenshteinDistanceColumnDetectEngine] cond1= {len(detailed_column_class_mapping)} < {float(len(backup_column_class_mapping) * self.threshold)}")
        print(f"[LevenshteinDistanceColumnDetectEngine] cond1={cond1} | cond2={cond2} | cond3={cond3}")

        if cond1 and cond2 and cond3:
            column_class_mapping = {k:"GenericColumn" for k in list(backup_column_class_mapping.keys())}
            print("[LevenshteinDistanceColumnDetectEngine] Fallback to GenericColumn")
        else:
            column_class_mapping = {k: v['class'] for k, v in detailed_column_class_mapping.items()}

        print(f"[LevenshteinDistanceColumnDetectEngine] result: {json.dumps(column_class_mapping, indent=4)}")
        return column_class_mapping

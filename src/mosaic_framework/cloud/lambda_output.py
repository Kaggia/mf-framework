################################################################################
# Module:      lambda_output.py
# Description: PostProcess component, used to flush model data into AWS Lambda 
#              Output.
# Author:      Stefano Zimmitti
# Date:        23/07/2024
# Company:     xFarm Technologies
################################################################################

from __future__ import annotations
from typing import List, TYPE_CHECKING

import os
import pandas as pd
from typing import Any
import warnings
import json

from mosaic_framework.config.configuration import SOURCE, LAMBDA_OUTPUT
from mosaic_framework.components.components import Component

if TYPE_CHECKING:
    from mosaic_framework.data_storage.data_storage import MosaicDataStorage
    from mosaic_framework.data_storage.shared_memory import MosaicSharedMemory
    MosaicDataStorageType  = MosaicDataStorage
    MosaicSharedMemoryType = MosaicSharedMemory


class LambdaOutput(Component):
    """
    This component is used to flush the model data into AWS Lambda Output. Helping
    to get the result from the Model, also allows to get the correct version of the
    result, 'compact' or 'detailed'.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(configuration=LAMBDA_OUTPUT, **kwargs)
        self.tag          = 'postprocess'
        self.data_storage : MosaicDataStorageType  = None
        self.shared_memory: MosaicSharedMemoryType = None
    
    @staticmethod
    def standardize_output(data:pd.DataFrame)->str:
        """
        This method is used to standardize the output, removing the unnamed columns
        and returning a dictionary representation of the DataFrame.
        :param data: DataFrame to be standardized.
        :return: Dictionary representation of the DataFrame.
        :rtype: str
        """
        for c in data.columns:
            if 'Unnamed' in c:
                data = data.drop(c, axis=1)
        return data.to_dict('records')
    
    @staticmethod
    def get_involved_connectors(connectors:List[dict], label:str)->List[dict]:
        """
        This method is used to get the connectors involved in the flow, based on the
        label passed.

        :param connectors: List of connectors.
        :param label: Label to filter the connectors.
        :return: List of connectors involved in the flow.
        :rtype: List[dict]
        """
        involved_connectors = []
        for i, _ in enumerate(connectors):
            if connectors[i]['connect_out'] == label:
                involved_connectors.append(connectors[i])
        return involved_connectors

    def run(self)->None:
        """
        This method is used to run the LambdaOutput component. It first gets the
        connectors involved in the flow, based on the label passed. Then, it filters
        the output based on the output format, compact or detailed. Finally, it
        flushes the output to a JSON file in the temp folder.

        :return: None
        :rtype: None
        """
        super().run()

        if self.output_format == 'default':
            self.output_format = 'compact'
        
        output = {'compact': [], 'detailed': []}

        #First, is step is about getting the connectors, cause they are the best
        #way to understand the flows of elaboration. We need to reconstruct the 
        #flow from top (Source) to bottom (LambdaOutput). 
        connectors_involved = \
            self.get_involved_connectors(
                connectors=self.shared_memory.get_variable(key='connectors').content,
                label=self.label)

        for c in connectors_involved:
            output['detailed'].append({
                'model': c['connect_in'],
                'data': self.standardize_output(self.data_storage.get_resource(label=c['resource']['results_pointer']).get_data())
            })
            output['compact'].append({
                'model': c['connect_in'],
                'data': self.standardize_output(self.data_storage.get_resource(label=c['resource']['compact_results_pointer']).get_data())
            })
        
        chosen_output = output[self.output_format]

        #Filtering output based on the elaborations, cause maybe there are two running
        #models, then we gotta flush to a single Json located in temp_folder, based on 
        #the current situation.

        #This is the classic setup, where a single model is run and results
        #can be easily returned.
        if len(chosen_output) == 1:
            chosen_output = chosen_output[0]['data']
        
        tmp_folder = self.shared_memory.get_variable(key='cloud_tmp_fld').content
        filepath   = f"{tmp_folder}/results/results.json"
        json.dump(chosen_output, open(filepath, "w+"), indent=4)

        print(f"[LambdaOutput]: Results flushed into: {filepath}.")
        print("[LambdaOutput]: Closed.")
        return

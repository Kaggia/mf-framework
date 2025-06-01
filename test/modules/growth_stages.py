################################################################################
# Module: growth_stages.py
# Description: Handling the growth stages got from:
#   https://kwb3qygq83.execute-api.eu-west-1.amazonaws.com/test/boundaries/get_growth_stages_data?commodity_id=1785&planting_id=0&precocity_id=mid.
# Author: Stefano Zimmitti
# Date: 11/01/2024
# Company: xFarm Technologies
################################################################################

from datetime import datetime
import requests
import json
import os

from modules.exceptions import GrowthStageFormatError

class GrowthStageData():
    def __init__(self, commodity_id:int, planting_id:int, precocity_id:str, disease_id:int) -> None:
        self.commodity_id = commodity_id
        self.planting_id  = planting_id
        self.precocity_id = precocity_id
        self.disease_id   = disease_id
    
    #Call the endpoint and get the data
    def get_data(self, stage:str='test'):
        api_codes = {'test': 'c6agzfpt4d','prod' : 'cf6lk4nz6c'} #insurtech
        api_keys  = {'test': 'iRG9zB7T995anJQ9tej1L1PwIWanDang68qtIao5',
                     'prod': 'KK0ngomQan5hhsIkfxJ8873FwKqWekWi3TQ1kmpr'}
        
        url       = f"https://{api_codes[stage]}.execute-api.eu-west-1.amazonaws.com/{stage}/boundaries/get_growth_stages_data?commodity_id={self.commodity_id}&planting_id={self.planting_id}&precocity_id={self.precocity_id}"

        r        = requests.get(url=url, headers={"x-api-key":api_keys[stage]})
        response = json.loads(r.text)
        
        #Catch Errors
        if 'message' in list(response.keys()):
            raise GrowthStageFormatError(f"Cannot retrieve Growth stages. Error message got: {response['message']}")
        
        return response['data']

    #elaborate get_data() results and returns a list
    #of growth stages expressed as ranges of days.
    def get_susceptible_growth_stages(self, year_context:int):
        gs_stage_pointer = os.getenv('growth_stage_pointer')
        raw_data         = self.get_data(gs_stage_pointer if gs_stage_pointer!=None else 'test')
        
        #remove susceptibility free growth stages and 
        #growth stages that are not susceptible to 
        #current disease id.
        filtered_growth_stages = [gs for gs in raw_data if len(gs['susceptibility'])>0 and len(set([susc['disease_id'] for susc in gs['susceptibility'] if susc['disease_id']==self.disease_id]))>0]
        #Contextualize (first) and check for across year
        context_growth_stages  = [{'gs_id': gs['gs_id'], 'gs_name': gs['gs_name'], 'start':str(year_context)+'-'+gs['start'], 'end':str(year_context if datetime.strptime(str(year_context)+'-'+gs['start'], '%Y-%m-%d')<datetime.strptime(str(year_context)+'-'+gs['end'], '%Y-%m-%d')else year_context+1)+'-'+gs['end']} for gs in filtered_growth_stages]
        return context_growth_stages
    
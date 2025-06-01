################################################################################
# Module: antrapi.py
# Description: Implementation of Antracnosi for Pisello.
# Author: Stefano Zimmitti
# Date: 26/02/2024
# Company: xFarm Technologies
################################################################################

#Needed to test | dev the library
import os 
import sys
os.chdir("../src")
sys.path.append(os.getcwd())


from modules.growth_stages import GrowthStageData
from modules.exceptions import BodyFormatError

from mosaic_framework.core.output_model import OutputModel
from mosaic_framework.core.math_factors import Equation, ApplyFunction
from mosaic_framework.core.value_factors import MappedValueOnTimeRangesRule
from mosaic_framework.core.reflection_factors import ReflectiveCondition, ReflectiveTimeframeCondition
from mosaic_framework.core.comparative_factors import (SimpleComparativeRule, 
    AndComparativeAgroRule, OrComparativeAgroRule, ComparativeTimeframeRule,
    ApplyAndBreakOnCondition)
from mosaic_framework.core.output_factors import SelectMaxAndCompare


import time
import json
import traceback
import pandas as pd
from datetime import datetime
import dateutil

#input weather params, got from the body
"""
    'temperature'  :  'hourly', 
    'precipitation':  'hourly', 
    'leaf_wetness' :  'hourly, expressed as 1 | 0'
"""

#Weather params needed for this model:
"""
    'temperature'  :  'expressed as °C, over an hour', 
    'precipitation':  'expressed as mm, over an hour', 
    'leaf_wetness' :  'expressed as minutes, over an hour'
"""
#Days worth of data needed for this model:
"""
Model strictly needs 2 [d-1, d] days of data in order to produce an
estimate for day d. Since a window (to compact the risk into [0, 4]) 
of [d-2, d-1, d, d+1, d+2] and previsional window is made up by 6 days 
ahead, we need to consider a wider window:
[d-3, d-2, d-1| d, d+1, d+2, d+3, d+4, d+5| d+6, d+7]
"""

def model(event):
    try:
        start_time = time.time()
        event = json.loads(event.get("body", "{}")) if isinstance(event['body'], str) else event['body']
        if not event:     raise BodyFormatError("Body is missing.")
        meas_list = event.get("measList", [])
        if not meas_list: raise BodyFormatError("Missing measList")

        #Get the Phenological Phases for the Model.
        growth_stage_data         = GrowthStageData(commodity_id=1462, planting_id=0, precocity_id='mid', disease_id=13)
        susceptible_growth_stages = growth_stage_data.get_susceptible_growth_stages(year_context=datetime.strptime(event['previsionDay'], '%Y-%m-%d').year)
        print(json.dumps(susceptible_growth_stages, indent=4, default=str))
        
        #Estimate the primary 
        #First we build the primary model by specifying
        #all the sub-models and rules that are needed to be applied

        ##############################################
        ##############    PRIMARY     ################
        ##############################################

        primary = OutputModel(
            label='primary', 
            previsionDay=dateutil.parser.parse(event['previsionDay']),
            data=pd.DataFrame(data=meas_list),
            history=(1, 0, 0), 
            risk_window=(2, 1, 2),
            prevision_window=(0, 1, 5),
            days=event['days'])
        
        #IT IS IMPORTANT AND MANDATORY ADD NEW FACTORS IN THE RIGHT ORDER
        #progressively the factors created have custom names, so we got to
        #target new columns with the right names
        #WARN: Possible bad targeting, columns automatic naming is the cause.
        #POSSIBLE SOLUTION: raise an error when column with the same name 
        #                   is detected, instead of automatic naming.

        #!IMPORTANT There is a minimum of 15 hours of precalculation for 'primary'
        #all must be set to 0.0, in order to fix the initial values

        #For each rule you can specify debug
        primary.add_factor(
            factor=SimpleComparativeRule(
                target='humidity', 
                column='leaf_wetness', 
                condition='goet87.0', 
                debug=True))
        primary.add_factor(
            factor=SimpleComparativeRule(
                target='rain', 
                column='rain_cwr_0', 
                condition='goet1.0', 
                debug=True))
        primary.add_factor(
            factor=Equation(
                column='equivalent_temperature',
                target='temperature',
                apply ='<temperature>/35.0',
                on_condition=AndComparativeAgroRule(
                    is_implicit=True,
                    rules=[
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='gt0', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='lt35.0', debug=True)], 
                    debug=True), 
                debug=True))
        primary.add_factor(
            factor=Equation(
                column='factor_2',
                target='equivalent_temperature',
                apply ='(4.929*(<equivalent_temperature>^1.36)*(1-<equivalent_temperature>))^4.663', 
                debug=True))
        primary.add_factor(
            factor=ApplyAndBreakOnCondition(
                column='factor_3', 
                target='leaf_wetness',
                fnc=sum, 
                break_condition='loet0', 
                reset_value=0.0, 
                debug=True))
        primary.add_factor(
            factor=Equation(
                target='factor_3',
                column='factor_lw_3',
                apply='(<factor_3>*0.021)-0.009',
                on_condition=SimpleComparativeRule(target='factor_3', is_implicit=True,condition='gt0', debug=True)))
        primary.add_factor(
            factor=MappedValueOnTimeRangesRule(
                column='factor_4', 
                target='sampleDate', 
                ranges=susceptible_growth_stages, 
                on='gs_id', 
                value={1:0.087, 2:0.942, 3:1.000, 'default':0.0}, 
                debug=True))
                
        primary.add_factor(
        factor=Equation(
            column='factor_5',
            target=['factor_2', 'factor_lw_3', 'factor_4'],
            apply="<factor_2>*<factor_lw_3>*<factor_4>",
            on_condition=OrComparativeAgroRule(column="factor_5_or", is_implicit=True, rules=[
                ComparativeTimeframeRule(column='factor_5_one_cond', target='rain_cwr_0', timeframe=24, aggregation_fnc=max, condition='gt0', debug=True),
                ReflectiveTimeframeCondition(target="factor_5", ref=[25, 1], aggregation_fnc=max, condition='gt0', is_implicit=True)], debug=True), 
            debug=True))
                
        primary.add_factor(
            factor=Equation(
                column='factor_6',
                target='temperature',
                apply ='((<temperature>-2)/19)*(((34-<temperature>)/13)^(13/19))',
                on_condition=AndComparativeAgroRule(
                    column="f6_and_comp_rule", 
                    rules=[
                        SimpleComparativeRule(target='factor_5',    is_implicit=True, condition='gt0', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='gt2', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='lt34',debug=True)]), 
                debug=True))


        primary.add_factor(
            factor=Equation(
                column='factor_7',
                target=['factor_6', 'factor_7[-1]'],
                apply ='(<factor_6>/150)+(<factor_7[-1]>)',
                on_condition=SimpleComparativeRule(target='factor_6', is_implicit=True, condition='gt0', debug=True), 
                debug=True))
        
        primary.add_factor(
        factor=Equation(
            column='factor_8',
            target=['factor_7', 'factor_8[-1]'],
            apply ='<factor_7>+<factor_8[-1]>',
            on_condition=AndComparativeAgroRule(is_implicit=True, rules=[
                SimpleComparativeRule(target='factor_5',is_implicit=True, condition='gt0', debug=True),
                ReflectiveCondition(target="factor_8", ref=1, condition='lt1', is_implicit=True)], debug=True), 
            debug=True))
        
        primary.set_output_rule(
            factor=SelectMaxAndCompare(
                column='primary', 
                target='factor_8', 
                condition='goet1.1', 
                ref=1))
        
        primary_side_data, primary_data = primary.estimate()
        ##############################################
        ##############    INTERMEDIATE     ###########
        ##############################################
        intermediate = OutputModel(
            label='intermediate', 
            data=pd.DataFrame(data=primary_side_data),
            previsionDay=dateutil.parser.parse(event['previsionDay']),
            history=(1, 0, 0), 
            risk_window=(2, 1, 2),
            prevision_window=(0, 1, 5),
            days=event['days'])

        #GOOD
        intermediate.add_factor(
            factor=Equation(
                column='factor_10',
                target='temperature',
                apply ='((<temperature>-2)/19)*(((34-<temperature>)/13)^(13/19))',
                on_condition=AndComparativeAgroRule(
                    column="f10_and_comp_rule", 
                    rules=[
                        SimpleComparativeRule(target='factor_8',    is_implicit=True, condition='gt0', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='gt2', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='lt34', debug=True)]), 
                debug=True))
        
        #GOOD
        intermediate.add_factor(
            factor=Equation(
                column='factor_11',
                target=['factor_10', 'factor_11[-1]'],
                apply ='(<factor_10>/168)+(<factor_11[-1]>)',
                on_condition=SimpleComparativeRule(target='factor_10',is_implicit=True, condition='gt0', debug=True), 
                debug=True))
        
        #GOOD
        intermediate.add_factor(
        factor=Equation(
            column='factor_12',
            target=['factor_11', 'factor_12[-1]'],
            apply ='<factor_11>+<factor_12[-1]>',
            on_condition=AndComparativeAgroRule(column="factor_12_condition", rules=[
                SimpleComparativeRule(target='factor_5',is_implicit=True, condition='gt0', debug=True),
                ReflectiveCondition(target="factor_12", ref=1, condition='lt1', is_implicit=True)], debug=True), 
            debug=True))
        
        intermediate.set_output_rule(
            factor=SelectMaxAndCompare(
                column='intermediate', 
                target='factor_12', 
                condition='goet1.1', 
                ref=1))
        
        intermediate_side_data, intermediate_data = intermediate.estimate()

        # ##############################################
        # ##############    SECONDARY     ##############
        # ##############################################
        secondary = OutputModel(
            label='secondary', 
            data=pd.DataFrame(data=intermediate_side_data),
            previsionDay=dateutil.parser.parse(event['previsionDay']),
            history=(1, 0, 0), 
            risk_window=(2, 1, 2),
            prevision_window=(0, 1, 5),
            days=event['days'])
        
        #NEED FACTOR_14    
        secondary.add_factor(factor=AndComparativeAgroRule(
            column='factor_14_cond_two', 
            rules=[
                ComparativeTimeframeRule(target='rain', is_implicit=True, timeframe=24, aggregation_fnc=sum, condition='gt0'),
                SimpleComparativeRule(target='factor_4', is_implicit=True, condition='gt0')],
            boolean_mapping={1:1, 0:-1}))
        
        secondary.add_factor(
            factor=ApplyFunction(
                column='factor_14_sum',
                target=['intermediate', 'factor_14_cond_two'],
                function=sum,
                debug=True))
        
        secondary.add_factor(
            factor=ApplyAndBreakOnCondition(
                column='factor_14_partial', 
                target='factor_14_sum',
                fnc=sum, 
                start_condition='goet2',
                break_condition='loet0', 
                reset_value=0.0, 
                debug=True))

        secondary.add_factor(
            SimpleComparativeRule(
                column='factor_14',
                target='factor_14_partial', 
                condition='goet2')
            )
        
        secondary.add_factor(
            factor=Equation(
                column='factor_15',
                target='equivalent_temperature',
                apply ='(5.2*((<equivalent_temperature>)^1.56)*(1-<equivalent_temperature>))^1.057',
                on_condition=SimpleComparativeRule(target='factor_14',is_implicit=True, condition='gt0', debug=True), 
                debug=True))

        secondary.add_factor(
            factor=Equation(
                column='factor_16',
                target='factor_3',
                apply ='1-1*((CONSTANT_E)^(-0.034*<factor_3>))',
                on_condition=SimpleComparativeRule(target='factor_14',is_implicit=True, condition='gt0', debug=True), 
                debug=True))
        
        secondary.add_factor(
        factor=Equation(
            column='factor_17',
            target=['factor_15', 'factor_16', 'factor_4'],
            apply="<factor_15>*<factor_16>*<factor_4>",
            on_condition=OrComparativeAgroRule(column="factor_17_or", rules=[
                SimpleComparativeRule(is_implicit=True, target='rain', condition='gt0', debug=True),
                ComparativeTimeframeRule(column='factor_17_one_cond', target='factor_14', timeframe=24, aggregation_fnc=max, condition='gt0', debug=True),
                ReflectiveTimeframeCondition(target="factor_17", ref=[25, 1], aggregation_fnc=max, condition='gt0', is_implicit=True)], debug=True), 
            debug=True))

        
        secondary.add_factor(
            factor=Equation(
                column='factor_18',
                target='temperature',
                apply ='((<temperature>-2)/19)*(((34-<temperature>)/13)^(13/19))',
                on_condition=AndComparativeAgroRule(
                    column="f18_and_comp_rule_0",
                    rules=[
                        SimpleComparativeRule(target='factor_17',   is_implicit=True, condition='gt0', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='gt2', debug=True),
                        SimpleComparativeRule(target='temperature', is_implicit=True, condition='lt34', debug=True)]), 
                debug=True))

        secondary.add_factor(
            factor=Equation(
                column='factor_19',
                target=['factor_18', 'factor_19[-1]'],
                apply ='(<factor_18>/150)+<factor_19[-1]>',
                on_condition=SimpleComparativeRule(target='factor_17',is_implicit=True, condition='gt0', debug=True), debug=True))

        secondary.add_factor(
        factor=Equation(
            column='factor_20',
            target=['factor_19', 'factor_20[-1]'],
            apply ='<factor_19>+<factor_20[-1]>',
            on_condition=AndComparativeAgroRule(is_implicit=True, rules=[
                SimpleComparativeRule(target='factor_17',is_implicit=True, condition='gt0', debug=True),
                ReflectiveCondition(target="factor_20", ref=1, condition='lt1', is_implicit=True)], debug=True), 
            debug=True))

        secondary.set_output_rule(
            factor=SelectMaxAndCompare(
                column='secondary', 
                target='factor_20', 
                condition='goet1.1',
                ref=1))
        
        secondary_side_data, secondary_data = secondary.estimate()

        result = pd.merge(
            left=primary_data, 
            right=secondary_data, 
            on=['sampleDate'])
        
        result['sampleDate'] = result['sampleDate'].apply(lambda x:x[:x.find(" ")])
                
    except Exception as e:
        print(traceback.format_exc())
        return {'result': "500_ERROR"}
    return {
        'result': "200_OK",
        "outputs":[
            {"primary_data":primary_data}, 
            {"intermediate_data":intermediate_data},
            {"secondary_data":secondary_data},
        ],
        "rules":[
            {"primary_side_data":primary_side_data},
            {"intermediate_side_data":intermediate_side_data},
            {"secondary_side_data":secondary_side_data},
        ]
    }

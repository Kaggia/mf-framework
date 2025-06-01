from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.validation.validator import Validator
from mosaic_framework.validation.activity import ModelValidation
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import Equation, ApplyFunctionOnRange,EquationOnCondition
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare, SelectMaxApplyAndComparison
from mosaic_framework.core.datetime_factors import isDayTimeRule, isNightTimeRule
from mosaic_framework.core.reflection_factors import ReflectiveCondition

Source(
    label='test_source', 
    environment="local",
    file="data_baroid.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='baroid_model')

Model(
    label="baroid_model",
    outputs=['infection'],
    history=5,
    granularity='daily',
    infection=[
                    
        AndComparativeAgroRule(
                column='inner_germination',
                rules=[
                    SimpleComparativeRule(target='max_temp', condition='loet25.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='min_temp', condition='goet2.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avg_humidity', condition='goet70.0',  is_implicit=True, debug=True)]),
                    
        ApplyFunctionOnRange(
            column='day_before_inner_germination',
            target='inner_germination',
            function=sum,
            range=[1,1],
            debug=True
            ),
            
        ApplyFunctionOnRange(
            column='day_before_before_inner_germination',
            target='day_before_inner_germination',
            function=sum,
            range=[1,1],
            debug=True
            ),
                    
        AndComparativeAgroRule(
            column='germination',
            rules=[
                SimpleComparativeRule(target='day_before_inner_germination', condition='goet1.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='day_before_before_inner_germination', condition='goet1.0',  is_implicit=True, debug=True)]),
    
        ApplyFunctionOnRange(
            column='day_before_germination',
            target='germination',
            function=sum,
            range=[1,1],
            debug=True
            ),
         
         AndComparativeAgroRule(
            column='sub_infection',
            rules=[
                SimpleComparativeRule(target='day_before_germination', condition='goet1.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='max_temp', condition='loet25.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='avg_temp', condition='goet8.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='avg_temp', condition='loet24.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='avg_humidity', condition='goet70.0',  is_implicit=True, debug=True)])
        ],
        
    infection_output_rule = SelectMaxAndCompare(column='infection', target='sub_infection', condition='goet1.0', ref=0))

from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.validation.validator import Validator
from mosaic_framework.validation.activity import ModelValidation
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import Equation, ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare, SelectMaxApplyAndComparison
from mosaic_framework.core.datetime_factors import isDayTimeRule, isNightTimeRule
from mosaic_framework.core.reflection_factors import ReflectiveCondition

Source(
    label='test_source', 
    environment="local",
    file="data_noccyt.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='noccyt_model')

Model(
    label="noccyt_model",
    outputs=['infection'],
    history=3,
    granularity= 'daily',
    infection=[
        DayOfYear(column='doy', target='sampleDate'),

        AndComparativeAgroRule(
                column='factor_start',
                rules=[
                    SimpleComparativeRule(target='doy', condition='goet121.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='doy', condition='loet243.0', is_implicit=True, debug=True)]),
         
        AndComparativeAgroRule(
                column='factor_germ',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet15.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet35.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgHumidity', condition='goet70.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True)
                ]),
                
        ApplyFunctionOnRange(
                column='factor_germ_fnc',
                target='factor_germ',
                range=(2, 0),
                function=sum
            ),
            
        AndComparativeAgroRule(
                column='factor_dis',
                rules=[
                    SimpleComparativeRule(target='rain', condition='goet3.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_germ_fnc', condition='goet1.0', is_implicit=True, debug=True)
                ]),
                
        ApplyFunctionOnRange(
                column='factor_dis_fnc',
                target='factor_dis',
                range=(2, 0),
                function=sum
            ),
        
        AndComparativeAgroRule(
                column='infection_result',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet18.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet32.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgHumidity', condition='goet80.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='leafWetness', condition='goet9.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_dis_fnc', condition='goet1.0', is_implicit=True, debug=True)
                ])
            ],
    infection_output_rule=SelectMaxAndCompare(
                column='infection',
                target='infection_result',
                condition='goet1.0',
                ref=0)
)

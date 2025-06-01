from mosaic_framework.environment.source import Source
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.environment.metadata import Metadata
from mosaic_framework.cloud.lambda_output import LambdaOutput
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import  ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.math_factors import Equation
from mosaic_framework.core.output_factors import SelectMaxAndCompare

Metadata(
    label='test_metadata',
    model='wherug',
    version='1.0.0',
    reference_crop='Wheat',
    reference_disease='BrownRust',
    agronomist='Vincenzo Tommaseo',
    developer='Lorenzo Gini', 
    tags=['stable', 'provider'])

Source(
    label='test_source', 
    environment="cloud",
    file="data.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='wherug_agro_model')

DataBridge(
    label='model_to_lambdaout_databridge',
    connect_in='wherug_agro_model',
    connect_out='output')

Model(
    label="wherug_agro_model",
    outputs=['infection'],
    history=5,
    granularity="daily",
    infection=[
        DayOfYear(column='doy', target='sampleDate'),
        SimpleComparativeRule(column='factor_start', target='doy', condition='loet143.0', debug=True),
        
        AndComparativeAgroRule(
            column='day_condition',
            rules=[
                SimpleComparativeRule(target='avg_temp', condition='loet28.0', is_implicit=True, debug=False),
                SimpleComparativeRule(target='avg_temp', condition='goet10.0', is_implicit=True, debug=False),
                SimpleComparativeRule(target='leaf_wetness', condition='loet24.0', is_implicit=True, debug=False),
                SimpleComparativeRule(target='leaf_wetness', condition='goet3.0', is_implicit=True, debug=False)
                ]),
        
        ApplyFunctionOnRange(
            column='day_before_condition',
            target='day_condition',
            function=sum,
            range=[1,1],
            debug=True
            ),
        ApplyFunctionOnRange(
            column='day_before_before_condition',
            target='day_before_condition',
            function=sum,
            range=[1,1],
            debug=True
            ),
        
        Equation(
          column='germination_partial',
          target=['avg_temp', 'leaf_wetness'],
          apply ='(-14.035 + 15.217*<avg_temp> - 0.586*<avg_temp>*<avg_temp> + 0.852*<leaf_wetness>)/100.0',
          on_condition=AndComparativeAgroRule(
            is_implicit=True,
            rules=[
            SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True),
            SimpleComparativeRule(target='day_before_condition', condition='goet1.0', is_implicit=True, debug=True)
                ])),
        Equation(
          column='germination',
          target='germination_partial',
          apply ='<germination_partial>',
          on_condition=SimpleComparativeRule(is_implicit=True, target='germination_partial', condition='gt0.0', debug=True)
            ),
        ApplyFunctionOnRange(
            column='day_before_germination',
            target='germination',
            function=sum,
            range=[1,1],
            debug=True
            ),
                
        Equation(
          column='apressorium_partial',
          target=['avg_temp', 'leaf_wetness'],
          apply ='(-60.125 + 15.339*<avg_temp> - 0.544*<avg_temp>*<avg_temp> + 3.401*<leaf_wetness>)/100.0',
          on_condition=AndComparativeAgroRule(
            is_implicit=True,
            rules=[
                SimpleComparativeRule(target='day_before_condition', condition='goet1.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='day_before_germination', condition='goet0.0', is_implicit=True, debug=True)
                ])),
        Equation(
          column='apressorium',
          target='apressorium_partial',
          apply ='<apressorium_partial>',
          on_condition=SimpleComparativeRule(is_implicit=True, target='apressorium_partial', condition='gt0.0', debug=True)
                ),
        ApplyFunctionOnRange(
            column='day_before_apressorium',
            target='apressorium',
            function=sum,
            range=[1,1],
            debug=True
            ),
            
        Equation(
          column='penetration_partial',
          target=['avg_temp', 'leaf_wetness'],
          apply ='(-159.867 + 17.423*<avg_temp> - 0.527*<avg_temp>*<avg_temp> + 5.787*<leaf_wetness>)/100.0',
          on_condition=AndComparativeAgroRule(
            is_implicit=True,
            rules=[
                  SimpleComparativeRule(target='day_condition', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='day_before_apressorium', condition='gt0.0', is_implicit=True, debug=True)
                ])),
        Equation(
          column='penetration',
          target='penetration_partial',
          apply ='<penetration_partial>',
          on_condition=SimpleComparativeRule(is_implicit=True, target='penetration_partial', condition='gt0.0', debug=True)
            ),
            
        Equation(
          column='infection_partial',
          target=['avg_temp', 'leaf_wetness'],
          apply ='(-159.867 + 17.423*<avg_temp> - 0.527*<avg_temp>*<avg_temp> + 5.787*<leaf_wetness>)/100.0',
          on_condition=AndComparativeAgroRule(
            is_implicit=True,
            rules=[
                  SimpleComparativeRule(is_implicit=True, target='leaf_wetness', condition='gt3.0'),
                  SimpleComparativeRule(is_implicit=True, target='penetration', condition='gt0.0')
                ])),
        Equation(
          column='sub_infection',
          target='infection_partial',
          apply ='<infection_partial>',
          on_condition=SimpleComparativeRule(is_implicit=True, target='infection_partial', condition='gt0.0', debug=True)
            )
            
            ],
        infection_output_rule=SelectMaxAndCompare(column='infection', target='sub_infection', condition='gt0.0', ref=0)
)

LambdaOutput(
    label="output",
    output_format='compact')
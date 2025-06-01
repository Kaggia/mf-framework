from mosaic_framework.environment.source import Source
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.environment.metadata import Metadata
from mosaic_framework.cloud.lambda_output import LambdaOutput
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import  ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxApplyAndComparison
from mosaic_framework.core.datetime_factors import isDayTimeRule, isNightTimeRule

Metadata(
    label='test_metadata',
    model='iwp',
    version='1.0.0',
    reference_crop='tomato',
    reference_disease='alternaria',
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
    connect_out='iwp_agro_model')

DataBridge(
    label='model_to_lambdaout_databridge',
    connect_in='iwp_agro_model',
    connect_out='output')

Model(
    label="iwp_agro_model",
    outputs=['infection'],
    history=48,
    granularity="hourly",
    infection=[
        DayOfYear(column='doy', target='sampleDate'),
        isDayTimeRule(column='is_day', target='sampleDate', range=[8,19]),
        isNightTimeRule(column='is_night', target='sampleDate', range=[8,19]),
        AndComparativeAgroRule(
            column='factor_start',
            rules=[
                SimpleComparativeRule(target='doy', condition='goet107.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='doy', condition='loet214.0', is_implicit=True, debug=True)]),
        AndComparativeAgroRule(
            column='day_hourly_condition',
            rules=[
                SimpleComparativeRule(target='humidity', condition='lt80.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='is_day', condition='goet1.0', is_implicit=True, debug=True)
                ]),
        AndComparativeAgroRule(
            column='night_hourly_condition',
            rules=[
                SimpleComparativeRule(target='humidity', condition='goet75.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True),
                SimpleComparativeRule(target='is_night', condition='goet1.0', is_implicit=True, debug=True)
                ]),
        ApplyAndBreakOnCondition(
            column='day_condition_subsequent_h_constructor',
            target='day_hourly_condition',
            fnc=sum,
            reset_value=0,
            start_condition= 'goet1.0',
            break_condition= 'lt1.0',
            debug=True
            ),
        ApplyAndBreakOnCondition(
            column='night_condition_subsequent_h_constructor',
            target='night_hourly_condition',
            fnc=sum,
            reset_value=0,
            start_condition= 'goet1.0',
            break_condition= 'lt1.0',
            debug=True
            ),
        ApplyAndBreakOnCondition(
            column='day_condition_subsequent_h',
            target='day_condition_subsequent_h_constructor',
            fnc=sum,
            reset_value=0,
            start_condition= 'et6.0',
            break_condition= 'lt6.0',
            debug=True
            ),
        
        ApplyAndBreakOnCondition(
            column='night_condition_subsequent_h',
            target='night_condition_subsequent_h_constructor',
            fnc=sum,
            reset_value=0,
            start_condition= 'et6.0',
            break_condition= 'lt6.0',
            debug=True
            ),
        SimpleComparativeRule(column='day_is_humidity_condition_satisfied', target='day_condition_subsequent_h', condition='et6.0', is_implicit=False, debug=True),
        SimpleComparativeRule(column='night_is_humidity_condition_satisfied', target='night_condition_subsequent_h', condition='et6.0', is_implicit=False, debug=True),
        ApplyFunctionOnRange(
            column='sum_day_condition_36h',
            target='day_is_humidity_condition_satisfied',
            function=sum,
            range=[36,0],
            debug=True
            ),
        ApplyFunctionOnRange(
            column='sum_night_condition_36h',
            target='night_is_humidity_condition_satisfied',
            function=sum,
            range=[36,0],
            debug=True
            )
    ],
    infection_output_rule=SelectMaxApplyAndComparison(
                column='infection',
                target=['sum_day_condition_36h','sum_night_condition_36h'],
                condition='goet2.0',
                ref=0,
                debug=True))

LambdaOutput(
    label="output",
    output_format='compact')
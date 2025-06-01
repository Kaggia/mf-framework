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
    model='manpoly',
    version='1.0.0',
    reference_crop='Almond',
    reference_disease='Polistygma',
    agronomist='Andrea Capoccia',
    developer='Stefano Zimmitti', 
    tags=['stable', 'provider'])

Source(
    label='test_source', 
    environment="cloud",
    file="data.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='manpoly_agro_model')

DataBridge(
    label='model_to_lambdaout_databridge',
    connect_in='manpoly_agro_model',
    connect_out='output')

Model(
    label="manpoly_agro_model",
    outputs=['infection'],
    history=4,
    granularity= 'daily',
    infection=[
        Equation(
                column='vpd',
                target=['avgTemp', 'avgHumidity'],
                apply ='(1-(<avgHumidity>/100))*6.11*(CONSTANT_E)^((17.47*<avgTemp>)/(239+<avgTemp>))',
                debug=True),

        DayOfYear(column='doy', target='sampleDate'),

        AndComparativeAgroRule(
                column='factor_start',
                rules=[
                    SimpleComparativeRule(target='doy', condition='goet60.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='doy', condition='loet240.0', is_implicit=True, debug=True)]),
        
        SimpleComparativeRule(column='cond_spo_dis', target='rain', condition='goet2.0', debug=True),
        
        AndComparativeAgroRule(
                column='spo_dis',
                rules=[
                    SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='cond_spo_dis', condition='goet1.0', is_implicit=True, debug=True)]),
        
        ApplyFunctionOnRange(
                column='germ_and_dev_appr_fnc',
                target='spo_dis',
                range=(2, 0),
                function=sum),
       
       AndComparativeAgroRule(
                column='germ_and_dev_appr',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet5.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet22.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(column='g_a_d_a_3', target='vpd', condition='loet4.0', debug=True),
                    SimpleComparativeRule(column='g_a_d_a_4', target='germ_and_dev_appr_fnc', condition='goet1.0', debug=True),
                    SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True, debug=True)]),
        
        ApplyFunctionOnRange(
                column='inf_gada_fnc',
                target='germ_and_dev_appr',
                range=(4, 0),
                function=sum),
        
        AndComparativeAgroRule(
                column='infection_result',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet10.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet22.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='vpd',  condition='loet4.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='rain', condition='goet0.2', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='inf_gada_fnc', condition='goet1.0', is_implicit=True, debug=True)
                ])
                ],
    infection_output_rule=SelectMaxAndCompare(
                column='infection',
                target='infection_result',
                condition='goet1.0',
                ref=0)
)

LambdaOutput(
    label="output",
    output_format='compact')
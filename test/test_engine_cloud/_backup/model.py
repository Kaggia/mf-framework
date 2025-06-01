from mosaic_framework.environment.metadata import Metadata
from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.cloud.lambda_output import LambdaOutput
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule
from mosaic_framework.core.math_factors import ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare

Metadata(
    label='test_metadata',
    model='noccyt',
    version='1.0.0',
    reference_crop='hazelnut',
    reference_disease='cytospora',
    agronomist='Andrea Capoccia',
    developer='Stefano Zimmitti', 
    tags=['stable', 'sensors'])

Colture(
    label='hazelnut',
    commodity_id=1753,
    destination_use_id=21,
    precocity_id=2102, 
    model_type="fixed")

Source(
    label='test_source', 
    environment="cloud",
    file="data.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='agro_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='hazelnut',
    connect_out='agro_model')

DataBridge(
    label='model_to_lambdaout_databridge',
    connect_in='agro_model',
    connect_out='output')

Model(
    label="agro_model", 
    outputs=['infection'],
    history=3,
    granularity='daily',
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
            function=sum),
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
            function=sum),
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
    output_infection_rule=SelectMaxAndCompare(column='infection', target='infection_result', condition='goet1.0', ref=0))

LambdaOutput(
    label="output",
    output_format='compact')
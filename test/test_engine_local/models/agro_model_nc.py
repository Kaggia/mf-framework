from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.agronomics.susceptibility import Susceptibility
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.validation.validator import Validator
from mosaic_framework.validation.activity import ModelValidation
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule
from mosaic_framework.core.math_factors import ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare, ApplyWindowing, ApplySusceptibility


Source(
    label='test_source', 
    environment="local",
    file="noccyt_data.json")

Source(
    label='validation_source', 
    environment="local",
    file="noccyt_result.xlsx")

Colture(
    label='apple',
    commodity_id=1481,
    susceptibility=Susceptibility(variety_id=148101, disease_id=0))

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='agro_model')

DataBridge(
    label='source_to_validator_databridge',
    connect_in='validation_source',
    connect_out='model_validator')

DataBridge(
    label='model_to_validator_databridge',
    connect_in='agro_model',
    connect_out='model_validator')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='apple',
    connect_out='agro_model')

Model(
    label="agro_model", 
    outputs=['infection'],
    history=3,
    granularity="daily",
    infection=[
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet121.0', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet243.0', is_implicit=True)], debug=True),
    ],
    infection_output_rule=[
        SelectMaxAndCompare(column='infection', target='factor_start', condition='goet1.0', ref=0, debug=True),
        ApplyWindowing(column='windowed_infection', target='infection', window_past=3, window_current=0, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='output', target='windowed_infection', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)
    ]
)

Validator(
    label='model_validator',
    source='validation_source',
    report=True,
    activity=ModelValidation(process='simple')
)
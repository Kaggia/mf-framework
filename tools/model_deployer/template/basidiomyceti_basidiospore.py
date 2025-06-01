from mosaic_framework.environment.metadata import Metadata
from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.agronomics.susceptibility import Susceptibility
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.cloud.lambda_output import LambdaOutput
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule
from mosaic_framework.core.math_factors import ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import ApplyWindowing, ApplySusceptibility

Metadata(
    label='metadata_component',
    model=<model_name>,
    version='1.0.0',
    reference_crop=<commodity_name>,
    reference_disease=<disease_name>,
    agronomist='Vincenzo Tommaseo | Andrea Capoccia',
    developer='Stefano Zimmitti', 
    tags=[<tags>])

Source(
    label='source_component', 
    environment="cloud",
    file="data.json")

Colture(
    label='colture_component',
    commodity_id=$commodity_id,
    susceptibility=Susceptibility(variety_id=$variety_id, disease_id=$disease_id))

DataBridge(
    label='source_to_model_databridge',
    connect_in='source_component',
    connect_out='basidiomyceti_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='colture_component',
    connect_out='basidiomyceti_model')

DataBridge(
    label='model_to_output_databridge',
    connect_in='basidiomyceti_model',
    connect_out='output')

Model(
    label="basidiomyceti_model", 
    outputs=['infection'],
    history=40,
    granularity="daily",
    infection=[
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet<doy_0_start>', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet<doy_0_end>', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='dispbasid', 
            rules=[
                SimpleComparativeRule(target='factor_start',    condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_w',    target='windSpeed',condition='goet<cond_w>'),
                SimpleComparativeRule(column='cond_rain', target='rain',condition='goet<cond_rain>')
        ]),
        AndComparativeAgroRule(
            column='cond_t', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_upper>', is_implicit=True),
                ], debug=True),
        ApplyFunctionOnRange(column='dispbasid_sum', target='dispbasid', range=[3, 0], function='sum'),
        AndComparativeAgroRule(
            column='germbasid', 
            rules=[
                SimpleComparativeRule(target='dispbasid_sum', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t',        condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_lw',       target='leafWetness',condition='goet<cond_lw>'),
                SimpleComparativeRule(column='cond_rh',       target='avgHumidity',condition='goet<cond_rh>')
        ]),
        ApplyFunctionOnRange(column='germbasid_infection_sum', target='germbasid', range=[1, 0], function='sum'),
        AndComparativeAgroRule(
            column='event_infection', 
            rules=[
                SimpleComparativeRule(target='germbasid_infection_sum', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True)
        ])
    ],
    infection_output_rule=[
        ApplyWindowing(column='windowed_risk_infection', target='event_infection', window_past=4, window_current=1, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='infection',   target='windowed_risk_infection', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)
    ]
)

LambdaOutput(
    label="output",
    output_format='compact')
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
    outputs=['primary', 'secondary'],
    history=40,
    granularity="daily",
    primary=[
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
        ApplyFunctionOnRange(column='germbasid_sum', target='dispbasid', range=[3, 0], function='sum'),
        AndComparativeAgroRule(
            column='germbasid', 
            rules=[
                SimpleComparativeRule(target='germbasid_sum', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t',        condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_lw',       target='leafWetness',condition='goet<cond_lw>'),
                SimpleComparativeRule(column='cond_rh',       target='avgHumidity',condition='goet<cond_rh>')
        ]),
        ApplyFunctionOnRange(column='germbasid_primary_sum', target='germbasid', range=[1, 0], function='sum'),
        AndComparativeAgroRule(
            column='event_primary', 
            rules=[
                SimpleComparativeRule(target='germbasid_primary_sum', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True)
        ])
    ],
    secondary=[
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start_3', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet<doy_1_start>', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet<doy_1_end>', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='cond_t_4', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_4_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_4_upper>', is_implicit=True),
                ], debug=True),
        ApplyFunctionOnRange(column='w_primary_sum', target='event_primary', range=[40, 0], function='sum'),
        AndComparativeAgroRule(
            column='spo', 
            rules=[
                    SimpleComparativeRule(target='w_primary_sum', condition='gt1.0',  is_implicit=True),
                    SimpleComparativeRule(target='cond_t_4', condition='goet1.0',  is_implicit=True),
                    SimpleComparativeRule(column='cond_lw_4',  target='leafWetness',condition='goet<cond_lw_4>'),
                    SimpleComparativeRule(column='cond_rh_4',  target='avgHumidity',condition='goet<cond_rh_4>'),
            ], debug=True),
        ApplyFunctionOnRange(column='spo_sum', target='spo', range=[6, 0], function='sum'),
        AndComparativeAgroRule(
            column='dispure_2', 
            rules=[
                    SimpleComparativeRule(target='factor_start_3', condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(target='spo_sum',        condition='goet2.0', is_implicit=True),
                    SimpleComparativeRule(column='cond_w_3',       target='windSpeed',condition='goet<cond_w_3>'),
                    SimpleComparativeRule(column='cond_rain_3',    target='rain',condition='goet<cond_rain_3>')
                ], debug=True),
        AndComparativeAgroRule(
            column='cond_t_5', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_5_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_5_upper>', is_implicit=True),
                ], debug=True),
        ApplyFunctionOnRange(column='dispure_2_sum', target='dispure_2', range=[3, 0], function='sum'),
        AndComparativeAgroRule(
            column='germure_2', 
            rules=[
                    SimpleComparativeRule(target='dispure_2_sum',  condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(target='cond_t_5',       condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(column='cond_lw_5',       target='leafWetness',condition='goet<cond_lw_5>'),
                    SimpleComparativeRule(column='cond_rh_5',        target='avgHumidity',condition='goet<cond_rh_5>')
                ], debug=True),
        
        ApplyFunctionOnRange(column='event_p_1', target='event_primary', range=[40, 3], function='sum'),
        AndComparativeAgroRule(
            column='event_p_2', 
            rules=[
                    SimpleComparativeRule(target='germure_2',      condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(target='factor_start_3', condition='goet1.0', is_implicit=True),
                ], debug=True),
        AndComparativeAgroRule(
            column='event_secondary', 
            rules=[
                    SimpleComparativeRule(target='event_p_1',   condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(target='event_p_2',   condition='goet1.0', is_implicit=True),
                ], debug=True),
    ],
    
    primary_output_rule=[
        ApplyWindowing(column='windowed_risk_primary', target='event_primary', window_past=4, window_current=1, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='primary',   target='windowed_risk_primary', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)
    ],
    secondary_output_rule=[
        ApplyWindowing(column='windowed_risk_secondary', target='event_secondary', window_past=4, window_current=1, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='secondary',   target='windowed_risk_secondary', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)
    ]
)

LambdaOutput(
    label="output",
    output_format='compact')
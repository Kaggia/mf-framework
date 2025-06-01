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
    connect_out='ascomyceti_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='colture_component',
    connect_out='ascomyceti_model')

DataBridge(
    label='model_to_output_databridge',
    connect_in='ascomyceti_model',
    connect_out='output')

Model(
    label="ascomyceti_model", 
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
            column='cond_t', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_upper>', is_implicit=True),
                ], debug=True),
        AndComparativeAgroRule(
            column='dispac', 
            rules=[
                SimpleComparativeRule(target='factor_start',    condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t',    condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_rh',   target='avgHumidity' , condition='goet<dispac_cond_rh>'),
                SimpleComparativeRule(column='cond_w',    target='windSpeed',condition='goet<dispac_cond_w>'),
                SimpleComparativeRule(column='cond_rain', target='rain',condition='goet<dispac_cond_rain>')
        ]),
        AndComparativeAgroRule(
            column='cond_t_2', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_2_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_2_upper>', is_implicit=True),
                ], debug=True),
        AndComparativeAgroRule(
            column='germasc', 
            rules=[
                SimpleComparativeRule(target='dispac', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t_2',   condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_lw',    target='leafWetness',condition='goet<germasc_cond_lw>'),
                SimpleComparativeRule(column='cond_rh_2',  target='avgHumidity',condition='goet<germasc_cond_rh_2>')
        ]),
        AndComparativeAgroRule(
            column='event_primary', 
            rules=[
                SimpleComparativeRule(target='germasc', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='factor_start', condition='goet1.0', is_implicit=True)
        ])
    ],
    secondary= [
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start_2', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet<factor_start_2_start>', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet<factor_start_2_end>', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='cond_t_3', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_3_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_3_upper>', is_implicit=True),
                ], debug=True),
        ApplyFunctionOnRange(column='primary_output_sum', target='primary', range=[40, 0], function='sum'),
        AndComparativeAgroRule(
            column='spo', 
            rules=[
                    SimpleComparativeRule(target='factor_start_2', condition='goet1.0', is_implicit=True),
                    SimpleComparativeRule(target='primary_output_sum', condition='goet1.0',  is_implicit=True),
                    SimpleComparativeRule(target='cond_t_3', condition='goet1.0',  is_implicit=True),
                    SimpleComparativeRule(column='cond_rh_3',  target='avgHumidity',condition='goet<spo_cond_rh_3>'),
                    SimpleComparativeRule(column='cond_lw_2',  target='leafWetness',condition='goet<spo_cond_lw_2>')
                ], debug=True),
        AndComparativeAgroRule(
            column='cond_t_4', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_4_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_4_upper>', is_implicit=True),
                ], debug=True),
        ApplyFunctionOnRange(column='spo_sum', target='spo', range=[40, 0], function='sum'),
        AndComparativeAgroRule(
            column='dispcon', 
            rules=[
                    SimpleComparativeRule(target='spo_sum', condition='goet1.0',  is_implicit=True),
                    SimpleComparativeRule(target='cond_t_4', condition='goet1.0',  is_implicit=True),
                    SimpleComparativeRule(column='cond_rh_4',  target='avgHumidity',condition='goet<dispcon_cond_rh_4>'),
                    SimpleComparativeRule(column='cond_ws_2',  target='windSpeed',condition='goet<dispcon_cond_ws_2>'),
                    SimpleComparativeRule(column='cond_rain_2',  target='rain',condition='goet<dispcon_cond_rain_2>')
                ], debug=True),
        ApplyFunctionOnRange(column='dispcon_sum', target='dispcon', range=[2, 0], function='sum'),
        AndComparativeAgroRule(
            column='cond_t_5', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet<cond_t_5_lower>', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet<cond_t_5_upper>', is_implicit=True),
                ], debug=True),
        AndComparativeAgroRule(
            column='gercon', 
            rules=[
                SimpleComparativeRule(target='dispcon_sum', condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t_5',    condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_lw_3',   target='leafWetness',condition='goet<gercon_cond_lw_3>'),
                SimpleComparativeRule(column='cond_rh_5',   target='avgHumidity',condition='goet<gercon_cond_rh_5>')
        ]),
        AndComparativeAgroRule(
            column='event_secondary', 
            rules=[
                SimpleComparativeRule(target='gercon',         condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='factor_start_2', condition='goet1.0', is_implicit=True)
        ])

    ],
    primary_output_rule=[
        ApplyWindowing(column='windowed_risk_primary', target='event_primary', window_past=4, window_current=1, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='primary',   target='windowed_risk_primary', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)

    ],
    secondary_output_rule=[
        ApplyWindowing(column='windowed_risk_secondary', target='event_secondary', window_past=4, window_current=1, window_future=0, window_fnc='sum', select_fnc='max'),
        ApplySusceptibility(column='secondary',     target='windowed_risk_secondary', select_fnc='max', grouping_fnc='sum', susceptibility_window=3)
    ]
)

LambdaOutput(
    label="output",
    output_format='compact')
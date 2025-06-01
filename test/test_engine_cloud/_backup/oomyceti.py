from mosaic_framework.environment.metadata import Metadata
from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.agronomics.susceptibility import Susceptibility
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.cloud.lambda_output import LambdaOutput
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, OrComparativeAgroRule
from mosaic_framework.core.math_factors import ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import ApplyWindowing, ApplySusceptibility

Metadata(
    label='metadata_component',
    model='oomyceti',
    version='1.0.0',
    reference_crop='',
    reference_disease='',
    agronomist='Vincenzo Tommaseo | Andrea Capoccia',
    developer='Stefano Zimmitti', 
    tags=['stable', 'sensor', 'disease'])


Source(
    label='source_component', 
    environment="cloud",
    file="data.json")

Colture(
    label='colture_component',
    commodity_id=1481,
    susceptibility=Susceptibility(variety_id=148101, disease_id=0))

DataBridge(
    label='source_to_model_databridge',
    connect_in='source_component',
    connect_out='oomyceti_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='colture_component',
    connect_out='oomyceti_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='oomyceti_model',
    connect_out='output')

Model(
    label="oomyceti_model", 
    outputs=['primary', 'secondary'],
    history=40,
    granularity="daily",
    primary=[
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet70.0', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet162.0', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='cond_t', 
            rules=[
                SimpleComparativeRule(target='minTemp', condition='goet8.0',  is_implicit=True),
                SimpleComparativeRule(target='maxTemp', condition='loet30.0', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='geros', 
            rules=[
                SimpleComparativeRule(target='factor_start', condition='goet1.0',  is_implicit=True),
                SimpleComparativeRule(target='cond_t',    condition='goet1.0',  is_implicit=True),
                SimpleComparativeRule(column='cond_rh',   target='avgHumidity', condition='goet75.0'),
                SimpleComparativeRule(column='cond_rain', target='rain',        condition='goet2.0'),
            ], debug=True),
        ApplyFunctionOnRange(column='geros_sum', target='geros', range=[4, 0], function='sum'),
        AndComparativeAgroRule(
            column='cond_t_2', 
            rules=[
                SimpleComparativeRule(target='minTemp', condition='goet10.0',  is_implicit=True),
                SimpleComparativeRule(target='maxTemp', condition='loet30.0', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='spo', 
            rules=[
                SimpleComparativeRule(target='geros_sum', condition='goet1.0',  is_implicit=True),
                SimpleComparativeRule(column='cond_rh_2', target='avgHumidity', condition='goet77.0'),
                SimpleComparativeRule(target='cond_t_2',  condition='goet1.0',  is_implicit=True),
            ], debug=True),
        ApplyFunctionOnRange(column='spo_sum', target='spo', range=[3, 0], function='sum'),
        AndComparativeAgroRule(
            column='disp', 
            rules=[
                SimpleComparativeRule(target='spo_sum',     condition='goet1.0',  is_implicit=True),
                OrComparativeAgroRule(
                    column='or_int_disp', 
                    rules=[
                        SimpleComparativeRule(column='cond_rain_2', target='rain', condition='goet2.0'),
                        SimpleComparativeRule(column='cond_wind',   target='windSpeed', condition='goet2.5'),
                    ])
            ], debug=True),

        AndComparativeAgroRule(
            column='cond_t_3', 
            rules=[
                SimpleComparativeRule(target='minTemp', condition='goet8.0',  is_implicit=True),
                SimpleComparativeRule(target='maxTemp', condition='loet30.0', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='gerzo', 
            rules=[
                SimpleComparativeRule(target='cond_t_3',  condition='goet1.0',  is_implicit=True),
                SimpleComparativeRule(column='cond_lw',   target='leafWetness', condition='goet5.0'),
                SimpleComparativeRule(target='disp',      condition='goet1.0',  is_implicit=True),
            ], debug=True),
        SimpleComparativeRule(column='event_primary',    target='gerzo',condition='goet1.0')
    ],
    secondary= [
        DayOfYear(column='doy', target='sampleDate', debug=True),
        AndComparativeAgroRule(
            column='factor_start_2', 
            rules=[
                SimpleComparativeRule(target='doy', condition='goet142.0', is_implicit=True),
                SimpleComparativeRule(target='doy', condition='loet244.0', is_implicit=True)], debug=True),
        AndComparativeAgroRule(
            column='cond_t_4', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet10.0', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet30.0', is_implicit=True),
                ], debug=True),

        ApplyFunctionOnRange(column='event_primary_sum', target='event_primary', range=[40, 0], function='sum'),
        AndComparativeAgroRule(
            column='spo_2', 
            rules=[
                SimpleComparativeRule(target='factor_start_2',   condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='event_primary_sum',condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t_4',         condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_rh_3',        target='avgHumidity',condition='goet75.0'),
                SimpleComparativeRule(column='cond_lw_2',        target='leafWetness',condition='goet5.0'),
            ], debug=True),
        ApplyFunctionOnRange(column='spo_2_sum', target='spo_2', range=[3, 0], function='sum'),
        AndComparativeAgroRule(
            column='disp_2', 
            rules=[
                SimpleComparativeRule(target='spo_2_sum',   condition='goet1.0', is_implicit=True),
                OrComparativeAgroRule(
                    column='or_int_disp_2', 
                    rules=[
                        SimpleComparativeRule(column='cond_rain_3', target='rain',     condition='goet3.0'),
                        SimpleComparativeRule(column='cond_wind_2', target='windSpeed',condition='goet2.5')                    ]),
            ], debug=True),
        ApplyFunctionOnRange(column='disp_2_sum', target='disp_2', range=[2, 0], function='sum'),
        AndComparativeAgroRule(
            column='cond_t_5', 
            rules=[
                    SimpleComparativeRule(target='minTemp', condition='goet10.0', is_implicit=True),
                    SimpleComparativeRule(target='maxTemp', condition='loet30.0', is_implicit=True),
                ], debug=True),
        AndComparativeAgroRule(
            column='gerzo_2', 
            rules=[
                SimpleComparativeRule(target='disp_2_sum',   condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(target='cond_t_5',   condition='goet1.0', is_implicit=True),
                SimpleComparativeRule(column='cond_rh_4', target='avgHumidity', condition='goet80.0'),
                SimpleComparativeRule(column='cond_lw_3', target='leafWetness', condition='goet5.0'),
            ], debug=True),
        SimpleComparativeRule(column='event_secondary',    target='gerzo_2', condition='goet1.0')
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




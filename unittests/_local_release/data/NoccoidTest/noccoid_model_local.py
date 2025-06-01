from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.validation.validator import Validator
from mosaic_framework.validation.activity import ModelValidation
from mosaic_framework.core.value_factors import ReferenceValue
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import Equation, ApplyFunctionOnRange,EquationOnCondition
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare, SelectMaxApplyAndComparison
from mosaic_framework.core.datetime_factors import isDayTimeRule, isNightTimeRule
from mosaic_framework.core.reflection_factors import ReflectiveCondition

Source(
    label='test_source', 
    environment="local",
    file="data_noccoid.json")

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='noccoid_model')

Model(
    label="noccoid_model",
    outputs=['primary', 'secondary'],
    history=7,
    granularity='daily',
    primary=[
        DayOfYear(column='doy', target='sampleDate'),
        
        AndComparativeAgroRule(
                column='factor_start',
                rules=[
                    SimpleComparativeRule(target='doy', condition='goet91.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='doy', condition='loet258.0', is_implicit=True, debug=True)]),
                    
        AndComparativeAgroRule(
                column='factor_spo_1',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet15.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet32.0',  is_implicit=True, debug=True)]),
                    
        SimpleComparativeRule(column='factor_spo_2', target='avgHumidity', condition='goet75.0', debug=True),
        
        AndComparativeAgroRule(
                column='sporulation',
                rules=[
                    SimpleComparativeRule(target='factor_start', condition='et1.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_spo_1', condition='et1.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_spo_2', condition='et1.0',  is_implicit=True, debug=True),
                    ]),
                    
        SimpleComparativeRule(column='factor_dis_1', target='rain', condition='goet3.0', debug=True),
        
        ReferenceValue(column='sporulation_lt_1', target='sporulation', ref=1, debug=True),
        
        Equation(
                column='sum_2_days_sporulation',
                target=['sporulation', 'sporulation_lt_1'],
                apply ='<sporulation>+<sporulation_lt_1>',
                rules=[
                    SimpleComparativeRule(target='factor_dis_1', is_implicit=True, condition='et1.0', debug=True)],
                debug=True),

        AndComparativeAgroRule(
                column='dispersion',
                rules=[
                    SimpleComparativeRule(target='sum_2_days_sporulation', is_implicit=True, condition='goet1.0', debug=True),
                    SimpleComparativeRule(target='factor_dis_1', is_implicit=True, condition='goet1.0', debug=True)],
                debug=True),


        AndComparativeAgroRule(
                column='factor_primary_1',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet16.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet28.0', is_implicit=True, debug=True)
                ]),
                
        SimpleComparativeRule(column='factor_primary_2', target='leafWetness', condition='goet10.0', debug=True),
        
        ReferenceValue(column='dispersion_lt_1', target='dispersion', ref=1, debug=True),
        
        
                
        Equation(
            target=['dispersion', 'dispersion_lt_1'],
            apply ='<dispersion>+<dispersion_lt_1>',
            column = 'sum_2_days_sporulation',
            rules=[
                SimpleComparativeRule(target='factor_primary_1', condition='et1.0',  is_implicit=True, debug=True),
                SimpleComparativeRule(target='factor_primary_2', condition='et1.0',  is_implicit=True, debug=True)]
                ),
                
        AndComparativeAgroRule(
                column='primary_result',
                rules=[
                    SimpleComparativeRule(target='sum_2_days_sporulation', is_implicit=True, condition='goet1.0', debug=True),
                    SimpleComparativeRule(target='factor_primary_1', condition='et1.0',  is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_primary_2', condition='et1.0',  is_implicit=True, debug=True)],
                debug=True),
                ],
                
    secondary=[
                
        AndComparativeAgroRule(
                column='factor_conidi_1',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet20.0', is_implicit=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet30.0', is_implicit=True)
                ]),
                
                
        AndComparativeAgroRule(
                column='factor_conidi_2',
                rules=[
                    SimpleComparativeRule(target='avgHumidity', condition='goet60.0', is_implicit=True),
                    SimpleComparativeRule(target='avgHumidity', condition='loet95.0', is_implicit=True)
                ]),
                
        ApplyFunctionOnRange(
                column='conidi_func',
                target='primary_result',
                range=(7, 5),
                function=sum
            ),
            
        AndComparativeAgroRule(
                column='conidi',
                rules=[
                    SimpleComparativeRule(target='conidi_func', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_conidi_1', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_conidi_2', condition='goet1.0', is_implicit=True, debug=True)
                ]),
                
        AndComparativeAgroRule(
                column='factor_secondary_1',
                rules=[
                    SimpleComparativeRule(target='avgTemp', condition='goet22.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgTemp', condition='loet29.0', is_implicit=True, debug=True)
                ]),
                
        AndComparativeAgroRule(
                column='factor_secondary_2',
                rules=[
                    SimpleComparativeRule(target='avgHumidity', condition='goet50.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='avgHumidity', condition='loet95.0', is_implicit=True, debug=True)
                ]),
                
        ApplyFunctionOnRange(
                column='factor_secondary_3_func',
                target='rain',
                range=(5, 1),
                function=sum
            ),
            
        SimpleComparativeRule(column='factor_secondary_3', target='factor_secondary_3_func', condition='loet2.5', debug=True),
        
        AndComparativeAgroRule(
                column='secondary_result',
                rules=[
                    SimpleComparativeRule(target='conidi',condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_secondary_1', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_secondary_2', condition='goet1.0', is_implicit=True, debug=True),
                    SimpleComparativeRule(target='factor_secondary_3', condition='goet1.0', is_implicit=True, debug=True)
                ])
            ],
    
    primary_output_rule = SelectMaxAndCompare(
                                            column='primary',
                                            target='primary_result',
                                            condition='goet1.0',
                                            ref=0),
        
    secondary_output_rule = SelectMaxAndCompare(
                                            column='secondary',
                                            target='secondary_result',
                                            condition='goet1.0',
                                            ref=0))

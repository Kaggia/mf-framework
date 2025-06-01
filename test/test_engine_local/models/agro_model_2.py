from mosaic_framework.environment.source import Source
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule
from mosaic_framework.core.math_factors import Equation
from mosaic_framework.core.output_factors import SelectMaxAndCompare


Source(
    label='test_source', 
    environment="local",
    file="antrapi_data.json")


DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='agro_model')

Model(
    label="agro_model", 
    outputs=['primary', 'secondary'],
    history=3,
    primary=[
        Equation(
            column='equivalent_temperature',
            target='temperature',
            apply ='<temperature>/35.0',
            on_condition=AndComparativeAgroRule(
            is_implicit=True,
            rules=[
                SimpleComparativeRule(target='temperature', is_implicit=True, condition='gt99'),
                SimpleComparativeRule(target='temperature', is_implicit=True, condition='lt99')
            ])
        ),
        SimpleComparativeRule(target='avg_temp', column='temp_p_rule_0', condition='goet1.0'),
        SimpleComparativeRule(target='min_temp', column='temp_p_rule_1', condition='goet1.0'),
        SimpleComparativeRule(target='max_temp', column='temp_p_rule_2', condition='goet1.0')], 
    secondary=[
        SimpleComparativeRule(target='avg_hum', column='temp_s_rule_0', condition='goet50.0'),
        SimpleComparativeRule(target='min_hum', column='temp_s_rule_1', condition='goet50.0'),
        SimpleComparativeRule(target='max_hum', column='temp_s_rule_2', condition='goet50.0')],
    primary_output_rule=SelectMaxAndCompare(column='primary', target='factor_8', condition='goet1.1', ref=1),
    secondary_output_rule=SelectMaxAndCompare(column='secondary', target='factor_8', condition='goet1.1', ref=1))
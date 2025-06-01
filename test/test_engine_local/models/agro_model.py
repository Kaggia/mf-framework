from mosaic_framework.environment.source import Source
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.validation.validator import Validator
from mosaic_framework.validation.activity import ModelValidation
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule
from mosaic_framework.core.math_factors import ApplyFunctionOnRange
from mosaic_framework.core.growth_models_factors import DayOfYear
from mosaic_framework.core.output_factors import SelectMaxAndCompare
from mosaic_framework.core.math_factors import Equation
from mosaic_framework.core.growth_models_factors import GDD, ActualPhenostageData, ActualPhenostageId, ActualPhenostageName, ActualPhenostageStart, ActualPhenostageEnd, ActualPhenostageUnit
from mosaic_framework.core.value_factors import MapValuesRule

Source(
    label='test_source', 
    environment="local",
    file="noccyt_data.json")

Colture(
    label='grapevine',
    commodity_id=1692,
    destination_use_id=7,
    planting_id=0,
    precocity_id=3,
    calendar_id=0,
    policy_type_id=3, 
    model_type='policy')

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='agro_model')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='grapevine',
    connect_out='agro_model')

Model(
    label="agro_model", 
    outputs=['infection'],
    history=3,
    granularity="daily",
    infection=[
        GDD(column='gdd', target=['maxTemp', 'minTemp'], min_temp=5.0, max_temp=35.0, cumulate=True, debug=True),
        ActualPhenostageData(column='actual_pheno_phase_id_0', target='gdd', parameter="phase_id", debug=True),
        ActualPhenostageId(column='actual_pheno_phase_id_1', target='gdd', debug=True),
        ActualPhenostageName(column='actual_pheno_phase_name_1', target='gdd', debug=True),
        ActualPhenostageStart(column='actual_pheno_phase_start_1', target='gdd', debug=True),
        ActualPhenostageEnd(column='actual_pheno_phase_end_1', target='gdd', debug=True),
        ActualPhenostageUnit(column='actual_pheno_phase_unit_1', target='gdd', debug=True),
        MapValuesRule(column='kc', mapping={'1330070':1.0, '1330071':1.1, '1330072':1.2, 'default':0.0}, debug=True),
        Equation(column='eq', target='gdd', apply='tan(cos(sin(<gdd>/1000*150)))', debug=True)
    ],
    infection_output_rule=SelectMaxAndCompare(column='infection', target='eq', condition='goet1.0', ref=0, debug=True))
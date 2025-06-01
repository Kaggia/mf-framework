from mosaic_framework.environment.source import Source
from mosaic_framework.model.model import Model
from mosaic_framework.retrieving.data_bridge import DataBridge
from mosaic_framework.agronomics.colture import Colture
from mosaic_framework.core.comparative_factors import SimpleComparativeRule, AndComparativeAgroRule, ApplyAndBreakOnCondition
from mosaic_framework.core.math_factors import Equation
from mosaic_framework.core.growth_models_factors import DayOfYear, GDD
from mosaic_framework.core.output_factors import SimpleOutputRule
from mosaic_framework.core.value_factors import Value, MapValuesRule, ReferenceValue
from mosaic_framework.core.growth_models_factors import GDD, ActualPhenostageId
from mosaic_framework.core.irrigation import IrrigationDeficit

Source(
    label='test_source', 
    environment="local",
    file="1322Meteo.json")

Colture(
    label='tomato',
    commodity_id=1528,
    destination_use_id=4,
    planting_id=0,
    precocity_id=3,
    calendar_id=0,
    policy_type_id=3, 
    model_type='policy')

DataBridge(
    label='source_to_model_databridge',
    connect_in='test_source',
    connect_out='Damage_drought')

DataBridge(
    label='colture_to_model_databridge',
    connect_in='tomato',
    connect_out='Damage_drought')


Model(
    label="Damage_drought",
    outputs=['ET0', "Fsolar","Ftemp", "Fheat","Fwater", 'YieldPotential', 'YieldReal','ModelResult'],
    history=5,
    granularity="daily",

    ET0=[
    ### location Lat e Long 
        Value(column="latitude",  value=42.167374), #"?"
        Value(column="longitude", value=14.45654), #"?"


    ### DOY calc
        DayOfYear(column="DOY", target="sampleDate", debug=True),

    ### ET0 determination calcs

        Equation(column="fi", target="latitude", apply="<latitude>*3.14159265/180", debug=True),
        Equation(column="delta", target="DOY", apply="0.409*sin((2*3.14159265/365)*<DOY>-1.39)", debug=True),
        Equation(column="omegas",target=["fi","delta"], apply="arccos(-tan(<fi>)*tan(<delta>))", debug=True),
        Equation(column="dr", target ="DOY", apply="1+0.033*cos((2*3.14159265/365)*<DOY>)", debug=True),
        Equation(column="Rad", target=["fi","delta", "omegas","dr"], apply="24*(60/3.14159265)*0.082*<dr>*(<omegas>*sin(<fi>)*sin(<delta>)+cos(<fi>)*cos(<delta>)*sin(<omegas>))", debug=True),
        Equation(column="ET0result", target=["Tmin", "Tmax","Tmean", "Rad"], apply="0.0023*(<Tmean>+17.8)*((<Tmax>-<Tmin>)^0.5)*<Rad>*0.408", debug=True)
    ],
    ET0_output_rule=SimpleOutputRule(column="ET0", target="ET0result", ref=0, debug=True),


#### Calcolo Funzione FSolar, che descrive il moltiplicatore giornaliero per la crescita della biomassa fotosintetizzante per la crescita giornaliera dell AGB
    Fsolar=[
        #### CROP FIXED VALUES
        Value(column="I50a", value=450.0), 
        Value(column="I50b", value=330.0),
        Value(column="Sco2", value=0.07),
        Value(column="FsolMax", value=0.95),
        Value(column="tsum", value=1450),
        ### CROP RELATED START E END DATE 
        Value(column="StartDoy", value = 101),
        Value(column="EndDoy", value = 275),
        ### CROP SIMPLIFIED FENOLOGICAL PHASES (RIPOSO, CRESCITA, SENESCENZA) IN GDD --> da aggiungere a DB 
        #Value(column="riposo", value=0.0),
        #Value(column="leaf_growth", value=1000.0),

        #calcolo gdd
        GDD(column='gdd', target=['Tmax', 'Tmin'], min_temp=10.0, max_temp=45.0, start_doy=101, cumulate=True, debug=True),
        
        #SimpleComparativeRule(column="riposo", target='gdd', condition='loet0.0', is_implicit=True, debug=False),
        AndComparativeAgroRule(
                    rules=[
                        SimpleComparativeRule(target='gdd', condition='gt0.0', is_implicit=True, debug=False),
                        SimpleComparativeRule(target='gdd', condition='loet1000.0', is_implicit=True, debug=False)],
                    column="leaf_growth",
                    debug=True),
        SimpleComparativeRule(column="senescence", target='gdd', condition='gt1000.0', debug=False),

        ### Calcolo Fsolar
        Equation(
                column="FSolar_leaf_growth", 
                target=["I50a","gdd","FsolMax"], 
                on_condition=SimpleComparativeRule(target="leaf_growth",condition="gt0",is_implicit=True),
                apply= "<FsolMax>/(1+2.71828^(-0.01*(<gdd>-<I50a>)))",
                debug=True), 
        Equation(
                column="FSolar_senescence", 
                target=["I50b","gdd","FsolMax","tsum"], 
                on_condition=SimpleComparativeRule(target="senescence",condition="gt0",is_implicit=True),
                apply= "<FsolMax>/(1+2.71828^(0.01*(<gdd>-(<tsum>-<I50b>))))",
                debug=True),
        Equation(
                column="FSolar_result", 
                target=["FSolar_leaf_growth","FSolar_senescence"], 
                apply= "<FSolar_leaf_growth>+<FSolar_senescence>",
                debug=True), 
    ],
    Fsolar_output_rule=SimpleOutputRule(column="Fsolar", target="FSolar_result", ref=0, debug=True),

### Calcolo Funzione Ftemp e fheat, che descrivono il fattore di correzione dell'emissione della biomassa dettato dalle basse temperature (Ftemp) e alle temperature alte (Fheat).
    Ftemp=[

    #### CROP FIXED VALUES
        Value(column="tBase", value=10.0),
        Value(column="tOpt", value=26.0),

        ### CALCOLO Ftemp 
        Equation(column="Ftemp1", target=["Tmean","tBase","tOpt"], 
            apply= "(<Tmean>-<tBase>)/(<tOpt>-<tBase>)",
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                    SimpleComparativeRule(target="Tmean",condition="gt10",is_implicit=True), 
                    SimpleComparativeRule(target="Tmean",condition="lt26",is_implicit=True)
            ],
            is_implicit=True,
            debug=True),
        debug=True),
        AndComparativeAgroRule(
            column="Ftemp2",
            rules=[
                SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                SimpleComparativeRule(target="Tmean",condition="gt26",is_implicit=True)
        ]),
        Equation(column="Ftemp_result", target=["Ftemp1","Ftemp2"], apply= "<Ftemp1>+<Ftemp2>",debug=True), 
    ],
    Ftemp_output_rule=SimpleOutputRule(column="Ftemp", target="Ftemp_result", ref=0, debug=True),

    Fheat=[ 
        #### CROP FIXED VALUES
        Value(column="tHeat", value=38.0),
        Value(column="tLetale", value=45.0),

            ### CALCOLO Ftemp 
        Equation(column="Fheat1", target=["Tmax","tHeat","tLetale"], 
            apply= "(<Tmax>-<tHeat>)/(<tLetale>-<tHeat>)",
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                    SimpleComparativeRule(target="Tmax",condition="gt38",is_implicit=True), 
                    SimpleComparativeRule(target="Tmax",condition="loet45",is_implicit=True)
                    ],
                is_implicit=True,
                debug=True),
            debug=True),
        AndComparativeAgroRule(
            column="Fheat2",
            rules=[
                SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                SimpleComparativeRule(target="Tmax",condition="loet38",is_implicit=True)
            ],debug=True),
        Equation(column="Fheat_result", target=["Fheat1","Fheat2"], apply= "<Fheat1>+<Fheat2>",debug=True), 
    ],
    Fheat_output_rule=SimpleOutputRule(column="Fheat", target="Fheat_result", ref=0, debug=True),

### Calcolo Funzione Fwater fulcro del modello, che permette di calcolare la risposta della crecita della biomassa a condizioni di stress idrico

    Fwater=[
        #### CROP FIXED VALUES: I VALORI RIGUARDANTI LE CARATTERISTICHE DEL SUOLO VARIANO SECONDO INPUT CLIENTE
        Value(column="TAW", value=50.0), #"?"
        Value(column="RAW", value=28.0),  #"?"
        Value(column="coeffIrr", value=0.85), #'?'
        ### Phenostages
        ActualPhenostageId(column='actual_pheno_phase_id', target='gdd', debug=True),
        ### kc values
        MapValuesRule(column="kc", target="actual_pheno_phase_id", 
            mapping="{'30152830041': 0.8, '30152830042': 1.0, '30152830043': 1.05,'30152830044': 0.9, '30152830045': 0.0,'default'  : 0.0}",debug=True),
        ### Calcolo ETC
        Equation(column="ETC", target=["ET0","kc"],
            apply= "<ET0>*<kc>",
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                ],
            is_implicit=True),
            debug=True),
                
        ### Calcolo Pioggia Utile
        MapValuesRule(column="PU_multiplier", target="actual_pheno_phase_id", 
            mapping="{'30152830041':1.0, '30152830042':1.0, '30152830043':1.0, '30152830044':1.0, '30152830045':0.0,'default':0.0}", debug=True),
        Equation(column="PU", target=["Rain","PU_multiplier"],
            apply= "<Rain>*<PU_multiplier>",
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                ],
                is_implicit=True),
            debug=True),

        AndComparativeAgroRule(
            column='doy_in',
            rules=[
                SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
            ],
            debug=True),
        
        AndComparativeAgroRule(
            column='fase_in',
            rules=[
                SimpleComparativeRule(target="gdd",condition="gt0.0",is_implicit=True),
                SimpleComparativeRule(target="gdd",condition="loet1000.0",is_implicit=True),
            ],
            debug=True),
        
        ReferenceValue(column="etc_previous_1", target="ETC", ref=1, debug=True),
        ReferenceValue(column="etc_previous_2", target="ETC", ref=2, debug=True),
        ReferenceValue(column="pu_previous_1",  target="PU",   ref=1, debug=True),
        ReferenceValue(column="pu_previous_2",  target="PU",   ref=2, debug=True),

        ReferenceValue(column="rain_prec_1",  target="Rain",   ref=1, debug=True),
        ReferenceValue(column="rain_prec_2",  target="Rain",   ref=2, debug=True),
        ReferenceValue(column="rain_prec_3",  target="Rain",   ref=3, debug=True),

        Equation(
            column="Rain_sum",  
            target=["rain_prec_1", "rain_prec_2", "rain_prec_3"],   
            apply='<rain_prec_1>+<rain_prec_2>+<rain_prec_3>',
            debug=True),
        
        ### Deficit idrico 
        # Equation(
        #     column="DI",
        #     target=['etc_prec_1', 'etc_prec_2', 'pu_prec_1', 'pu_prec_2', 'fase_in', 'doy_in', 'Rain_sum', 'TAW', 'RAW', 'coeffIrr','DI[-1]', 'DI[-2]'],
        #     apply='(<fase_in>*<doy_in>)*(((min(1,(<TAW>-<DI[-1]>)/(<TAW>-<RAW>))*<etc_prec_1>)-<pu_prec_1>+<DI[-1]>)-(((0.5*(1+sign((<TAW>-<DI[-1]>)-5)))*(0.5*(1+sign(<DI[-1]>-<RAW>)))*(0.5*(1+sign(5-<Rain_sum>)))*(((min(1,(<TAW>-<DI[-2]>)/(<TAW>-<RAW>)))*<etc_prec_2>-<pu_prec_2>+<DI[-2]>)*<coeffIrr>)+((0.5*(1+sign(5-(<TAW>-<DI[-1]>))))*(<TAW>/2)))))',
        #     #apply='(<fase_in>*<doy_in>)*(((min(1,(<TAW>-<DI[-1]>)/(<TAW>-<RAW>))*<etc_prec_1>)-<pu_prec_1>+<DI[-1]>))',
        #     debug=True
        # ), 
        IrrigationDeficit(
            column="DI",
            target=["etc", "pu"],
            taw=50.0,
            raw=28.0,
            irrigation_coefficient=0.85,
            previous_data_index=2,
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                ],
                is_implicit=True
            ),
            debug=True
        ),
        ### Calcolo Ti 
        Equation(
            column="Ti", target=["DI","TAW","ETC"],
            apply= "min(<ETC>,max(min(<TAW>-<DI>,<TAW>),0)*0.096)",
            debug=True
        ),
        ### Calcolo Arid
        Equation(
            column="Arid", target=["Ti","ETC"],
            apply= "max(1-(<Ti>/<ETC>),0)",
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="Ti",condition="gt0",is_implicit=True),
                    SimpleComparativeRule(target="ETC",condition="gt0",is_implicit=True),
            ],
            is_implicit=True,
            debug=True
        ),debug=True),
        Value(column="Swater", value=0.8, debug=True),
        ### Calcolo Fwater_result
        Equation(
            column="Fwater_result", target=["Arid","Swater"],
            apply= "max(1-(<Swater>*<Arid>),0)",
            debug=True
        ),
    ],
    Fwater_output_rule=SimpleOutputRule(column="Fwater", target="Fwater_result", ref=0, debug=True),

### CALCOLO AGB e Yield Potential --> NO stress di Fwater 
    YieldPotential=[
        Value(column="RUE",value=1.18), #"?" attenzione, questo valore deve essere variato proporzionalmente rispetto alla resa target imposta dall'utente
        Value(column='HI',value=0.63), #"?"
        MapValuesRule(column="Fwater_NOStress", target="actual_pheno_phase_id", 
            mapping="{'30152830041':1.0, '30152830042':1.0, '30152830043':1.0, '30152830044':1.0, '30152830045':0.0,'default':0.0}", debug=True),

        Equation(
            column="AGBPotential",
            target=['Rad','Fsolar','Ftemp','Fheat','Fwater_NOStress','RUE'],
            apply="<Rad>*<RUE>*<Fsolar>*<Ftemp>*min(<Fwater_NOStress>, <Fheat>)",
            debug=True
        ), 
        ApplyAndBreakOnCondition(
            column="AGBPotSum",
            target='AGBPotential',
            start_condition="gt0",
            break_condition="lt-1",
            reset_value=0,
            fnc='sum',
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                ],
                is_implicit=True
            ),
            debug=True
        ),

        Equation(
            column="YieldPotResult",
            target=['HI', 'AGBPotSum'],
            apply='<HI>*<AGBPotSum>',
            debug=True
        ),
    ], 
    YieldPotential_output_rule=SimpleOutputRule(column="YieldPotential", target="YieldPotResult", ref=0, debug=True),

### Yield Real, output reale del modello
    YieldReal=[
        MapValuesRule(column="YieldReal_block", target="actual_pheno_phase_id", 
            mapping="{'30152830041':1.0, '30152830042':1.0, '30152830043':1.0, '30152830044':1.0, '30152830045':0.0,'default':0.0}", debug=True),

        Equation(
            column="AGBReal",
            target=['Rad','Fsolar','Ftemp','Fheat','Fwater','RUE', 'YieldReal_block'],
            apply="<Rad>*<RUE>*<Fsolar>*<Ftemp>*min(<Fwater>, <Fheat>)*<YieldReal_block>",
            debug=True
        ), 
        ApplyAndBreakOnCondition(
            column="AGBRealSum",
            target='AGBReal',
            start_condition="gt0",
            break_condition="lt-1",
            reset_value=0,
            fnc='sum',
            on_condition=AndComparativeAgroRule(
                rules=[
                    SimpleComparativeRule(target="DOY",condition="goet101",is_implicit=True),
                    SimpleComparativeRule(target="DOY",condition="loet275",is_implicit=True),
                ],
                is_implicit=True
            ),
            debug=True
        ),
        Equation(
            column="YieldRealResult",
            target=['HI', 'AGBRealSum'],
            apply='<HI>*<AGBRealSum>',
            debug=True
        ),
    ], 
    YieldReal_output_rule=SimpleOutputRule(column="YieldReal", target="YieldRealResult", ref=0, debug=True),

###ModelResult- risultato del modello, ossia la differenza netta tra yield real e il minore tra la resa obiettvo (yield target) e la resa potenziale (yield pot)
    ModelResult=[
        Value(column="YieldTarget", value=800), #"?"
        Equation(
            column="deltaTargetReal",
            target=['YieldTarget','YieldReal'],
            apply="<YieldTarget>-<YieldReal>",
            debug=True
        ),
        Equation(
            column="deltaPotReal",
            target=['YieldPotential','YieldReal'],
            apply="<YieldPotential>-<YieldReal>",
            debug=True
        ),
        Equation(
            column="deltaMin",
            target=['deltaTargetReal','deltaPotReal'],
            apply='min(<deltaTargetReal>,<deltaPotReal>)',
            debug=True
        ),
    ],
    ModelResult_output_rule=SimpleOutputRule(column="ModelResult", target='deltaMin', ref=0, debug=True)
)
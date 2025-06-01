# Mosaic Framework

## Changes
### Version 0.0.1a
Questa versione comprende gli sviluppi iniziali del framework, che nella sua versione base era composta da semplici moduli interni al modulo di difesa. Adesso è stata convertita una libreria, che potrà essere installata su EFS.

### Version 1.0.0b0
- ReflectiveAgroRule moved into new module.
- Circular import fix.
- Refactoring of local printing of results. 
- Reflection detection in rule and condition evaluation. 
- Each Rule now has 'on_condition' (Not actually calculated)
- on_condition is precalculated in AgroRule evaluate.
- Equation has been deprecated, now EquationOnCondition is the new Equation class (same job).
- Refactoring of old reflection detection, now is automatic, and done each time a new rule is added to the model.
- Officially dropped supporting to string-conditions. (they are used as base case, in on_condition param)
- Added automatic-filtering by on_condition.
- Fixed bad inheritance for Reflective factors

### Version 1.0.0b1
- Added unittests
- Added debug print to recognize an EmptyAgroRule column on on_condition.

### Version 1.0.1.20240517
- Added ModuleParser to parse a single module, matching every module in MosaicFramework library.
- Added Components module.
- Added Component parent class, in order to manage common methods on the components classes.
- Added fill_parameter_methods, gathering all functions used to auto-fill params in components classes.
- Added configuration file, to gather all configurations about components.
- Added, alpha version, of MosaicEngine, entry point of elaboration.
- Added source module, within we added Source class to handle all data, actors and environment.
- Added further parsing for objects found in the analyzed model (usually written by an agronomist)
- Added automatic building library on Engine testing.

### Version 1.0.2.20240524
- Added MosaicDataStorage class, let the elaboration, data and configuration persist.
- Added set_storage function in MosaicDataStorage.
- Added set_storage function in Component.
- Added WRITERS_MAPPING in configuration.
- Added add_resource, get_resource in MosaicDataStorage.
- Added Resource class, which maps a file, with info in it, that can be retrieved and used during the elaboration.
- Added writers class, TxtWriter and JsonWriter in it. To write txt and json files in MosaicDataStorage.
- Added in MosaicEngine.run() a reference to the agromodel name, saved in MosaicDataStorage
- Setting storage in MosaicEngine.run() all object present.
- skip_autofill function added into fill_parameter_method.
- READERS_MAPPING added to mappings in config.
- readers module added with readers in it.
- get_data function added to Resource.
- flushing data from local to MosaicDataStorage.
- smart typing, avoid circular imports.
- Added Processors.
- Added baseline method run for Processors.
- WRITERS_MAPPING and READERS_MAPPING updated in configuration.
- CsvReader implemented.
- CsvWriter implemented.
- Added local behaviour with no file specified, for Source component.

### Version 1.0.3.20240527
- Added MosaicSharedMemory, a shared memory across the engine.
- Added SharedVariable, that maps a single shared information, stored in MosaicSharedMemory.
- Source now adds the environment as SharedVariable.
- MosaicSharedMemory is now injected into all Components.
- Processors are instancied inside run function of MosaicEngine.
- set_memory function added to Component, granting injection into components.
- Updated function to get and update a SharedVariable.
- New ModuleParser with updated parsing capability, it can parse more AST objects than before. 
- Fixed packaging
- Configuration new structure, with metadata and params keys.
- Validation revamped including new configuration structure.
- Filtering components when injecting in MosaicEngine.
- First version of RuleParser (as algorithm in Model).
- Removed rules parsing and added to its own module.
- ModuleParser changed to ComponentParser.

### Version 1.0.4.20240605
- Added DataBridge, handler of connection between Components.
- Added SubComponent, as minor runtime Component instanciated.
- Added Connector, as minor runtime Component, needed to connect two Components.
- Added SourceToModelConnector, as minor runtime Component, needed to connect two a Source and a Model Components, in order to share data.
- DATA_BRIDGE as configuration added
- get_variable fixed in MosaicSharedMemory
- injection of MosaicSharedMemory into processors
- PreProcessor implementation
- get_data function for Model, getting data from SharedMemory (resource pointer) and DataStorage (resource-itself)
- JsonReader now has an implementation for read function
- Model validation for outputs and data.
- New GDD rule on new module GrowthModels.+
- Model configurations for params updated.
- ApplyFunctionOnRange new rule added.
- ReferenceValue new rule added.
- DayOfYear new rule added.

### Version 1.0.5.20240619
- Configurations MODEL fixed
- OutputModel adaptation to the current situation of Mosaic
- ComponentParser adapted to parse objects in params
- Added OutputRuleParser, allowing to parse OutputRule
- Model Component completed.

### Version 1.0.6.20240705
- Added InternalComponent, it's just a stub.
- VALIDATOR, MODEL_VALIDATION_ACTIVITY and readers, writers updated in config module.
- MOSAIC_FRAMEWORK_LOCAL_PATHS added in new module local_configuration.
- validation literals module added.
- DayOfYear rule fixed missing target param.
- changed names to more general purpose in output_modules in core.
- ApplyFunctionOnRange fixed target needed column.
- Added raise on resource not found if policy is raise, in data storage.
- ExcelReader implemented
- Added raise on shared variable not found if policy is raise, in shared memory.
- CsvWriter updated implementation.
- parser now parse also ast. tuples.
- Processors skip InternalComponent elaboration.
- Fixed error on multiple source definition, in Source.
- Implemented communication results to other components, in order to be elaborated by Validator mainly.
- SourceToValidatorConnector, ModelToValidatorConnector connectors available to be used.
- ModelValidation activity implemented, in order to allow validate Model's results, crossing them with validation results.
- Added IsColumnInplace atomic validation.
- SimpleModelValidator added, collecting a series of atomic validation processes
- Validator Component added, allowing to launch all the validation suite.

### Version 1.0.7.20240716
- Added replace policies for validation.
- Atomic validation updated with replace policies.
- Added docs for Engine.
- Added basic docs for Rules.

### Version 1.1.0.20240719
- Source Component updated to support local temp folder data handling, for cloud
AWS Lambda elaborations.
- Build library script fixed on new feature or version release.
- Fixed message in exception raised by SharedMemory.
- Added test case for Cloud (local).

### Version 1.1.1.20240723
- Added LambdaOutput Component to help out to get the result of the elaboration.
- Added configurations for LambdaOutput Component.
- ComponentParser now works properly on Cloud Environment.
- Fixed Source Component in Cloud environment.
- Added ModelToLambdaOutputConnector to map DataBridge case for Model and LambdaOutput.

### Version 1.1.2.20240724
- Metadata Component added 

### Version 1.1.3.20240805
- Colture Component added, gathering all Agronomics stuff of the framework.
- GrowthModel(s) SubComponent added, to map different kind of growth models, that are directly connected with growth model service.
- FixedGrowthModel SubComponent added, to map the case where we want to build a model that use fixed date growth model.
- GddGrowthModel stub added.
- Components now have priorities on running, from 0(most important) to 9 (less important).
- Components now can be uniquely defined.
- COLTURE configuration added.
- data_layer_configuration module added.
- GetApi class to handle API calls added
- remove_resource and replace_resource are now available for the DataStorage.
- remove function is now available for Resource.
- validate_components function added to processors in order to check for unique Components (eg. Colture).
- fixed get_default_date_column in Model Component.
- ColtureToModelConnector added
- DataBridge now has priority 0.
- Now secrets from AWS can be retrieved and processed, through, 
MosaicVault that stores secrets, and Secret that map a single secret retrieved and processed.

### Version 1.1.4.20240805
- New mappings for Mosaic secrets stored in AWS Secret Manager.
- Raising exception when a mapping for a secret is not find.
- New Rule: MapValuesRule, it allows to map values into others.
- Updated Rule: MappedValueOnTimeRangesRule, it allows to map values into others, based on a timeranges mapping.
- unittests updated and got better.
- Added tags as configuration for METADATA
- Fixed start_date missing value
- Fixed build method for GetApi SubComponent.
- Fixed mapping in MosaicVault.
- Updated build_and_deploy tool, faster debug.
- Added GeospatialSource as component, it maps latitude and longitude into MosaicSharedMemory.
- Added PyWriter and PyReader
- Added PyWriter to write MosaicPipeline files
- Added PyReader to read MosaicPipeline files
- RawParser class implementation, read a .py file and replace each occurence found in it, found in params.
- RawParser implementation: Allows to pre-process the MosaicPipeline file in order to replace some parameters with "$" notation. Dumping result into MosaicDataStorage.
- ComponentParser updated: Get the model from MosaicDataStorage, preparse eventually by RawParser.

### Version 1.1.5.20240924
- Added get_mapped_function added in ApplyAndBreakOnCondition, to avoid miss-parsing.
- Partially fixed ast.UnaryOp bad parsing. Positive-Integer values are correctly parsed, but not the negative ones.
- SelectMaxApplyAndComparison added to set of Rules: Allows to Select the max, of each selected target column, in a day worth of data and check condition on each column.
- apply_condition_over_values added as function, to help processing row by row.
- Added new Rule: getDayRule: retrive the day of the year from the date column.
- Added new Rule: getHourlyRule: retrive the hours of the day from the date column (useful in hourly models).
- Added new Rule: isDateTime: flag with 1 the value that are inside a range. Mainly used to identify the light hours in the day (hourly models).
- Added new Rule: isNightTime: flag with 1 the value that are outside a range. Mainly used to identify the dark hours in the day (hourly models).
- Fixed date, in version, change on debug mode. Now is fixed to old date if debug is True.
- Model component has a new parameter 'granularity' where we estabilish what is the Model working granularity.
- Added the TestMadeWeatherProvider class that wrap the getApi, specifically for the made.
- Developed two engine (DynamicMappingEngine, ConstantMappingEngine), to map dynamicly or staticly the columns of a pandas dataframe to the one in the framework.
- CONVERTERS_MAPPING added to the configurations.
- Converter class added, it allows to convert data to a Resource useful format, or eventually a data useful format.
- Converter class includes also convert_from_json_to_dataframe.
- ModuleParser added to parse a module and get the classes in it.
- Columns engine added to Source component, both for validation and input pipeline.
- LevenshteinDistance implemented to detect columns from a set of classes in a module.
- Set of default columns added within a module columns.
- Input data validation processing, comprehending filling policies, based on what type of missing data you found.
- InputDataFiller implementations (part of Input data validation processing).
- InputDataValidator implementations (part of Input data validation processing).
- Convertion to json fixed.
- Fixed dropping source input file into MosaicDataStorage.
- convert_from_dataframe_to_xlsx added.
- Source Component checks if input is fillable or not before effectively fill it.
- Fixed name in GrowthModel SubComponent. 
- Converter configurations updated.
- Converter convert_from_json_to_dataframe is more general.
- Detect engine is now more precise
- Columns now have other names to be referred to
- Fixed bad update of a SharedVariable in Source, referring to Column mappings.
- Added reference, for SubComponents of their parent.
- GrowthModel dynamically retrieve the SampleDate column
- Converter now can converts to xslx using buffers.
- ExcelWriter work for both bytes and ExcelFile from pandas
- Fixed forwarding results through multiple outputs submodels.
- removed warnings launched by Source Component.

### Version 1.2.0
- Added IsDaylight and DataSource as detectable columns.
- Fixed few unittests relative to Made.
- JsonReader is now more flexible, getting data from a key in a dict or directly the list of values in a json.
- DatetimeParser integrated into Source Component.
- OutputModel behave strangely based on the content of Datetime-like columns, so to avoid any inconvicience we added a parsing of the column relative to datetime.
- DatetimeParser class implementation, it grants to convert batch and single datetime expressed as strings.
- Removed every single reference to infection models. Now generic models could be done.
- New ApplyWindowing OutputRule, allow to apply a function on a window series of data.
- RulesHub may return a default value if missing.
- granularity added to the RulesHub once got in the Model Component.
- Fixed bug on on_condition inplace.
- OutputModel supports multiple OutputAgroRule
- New mapped columns are available: GrowingDegreeDays, CumulatedGrowingDegreeDays, GrowingDegreeHourly, CumulatedGrowingDegreeHourly, WindSpeed, WindDirection
- Colture Component supports susceptibility SubComponent.
- It is possible to get the current env var for the stage.
- Susceptibility Component, allows to retrieve Susceptibility data from DiseaseV2 module.
- ApplySusceptibility OutputAgroRule implementation.
- Fixed GetApi class, when there are no querystrings.
- Disease API mappings added to the MosaicVault.
- ApplyFunctionOnRange now can set a default by choosing filling policy: coerce and default.
- Fixed Compact result, by filtering results.
- Fixed bug on merging results on primary and secondary use case (Disease)
- Fixed bug on fixing results the exceed the risk limit (4.0)

### Version 1.2.1
- Docstrings for Core module have been updated and refactored.
- Component now allows to set a default value for the column, also for not "default" values.
- Colture now integrates the chance to get GrowthStages from Insurtech module.
- PolicyGrowthModel allows to get the growth stages from the insurtech module.
- GDD rule now can cumulate the values.
- ActualPhenostageData can calculate the current growth stage, and get a specified parameter.
- ActualPhenostageId, ActualPhenostageName, ActualPhenostageStart, ActualPhenostageEnd, ActualPhenostageUnit have been defined as wrapper of ActualPhenostageData, actually retrieving the parameter "phase_id", "phase_name", "phase_start", "phase_end", "phase_unit".
- Equation now can be used with basic math functions (such as sin, cos, tan, exp, log, sqrt, etc.)
- GrowthModel-like classes retrieve and load data into VLookUp table, into the SharedMemory, then it is used by the rules (passed to RulesHub).
- Added Insurtech data to the MosaicVault.

### Version 1.2.2
- [Fix] Added rule to handle DamageDrought.

### Version 1.2.3
- [Fix] ApplySusceptibility: fixed bug on susceptibility calculation.

### Version 1.2.4
- [Fix] ApplyWindowing: fixed bug on windowing calculation.
- [Fix] ApplySusceptibility: fixed bug on susceptibility calculation, reporting the correct values on checks when the cycle's index is not in the window.

## Testing
### 2- Engine | Local
L'engine è possibile testarlo solamente previa installazione del package. Per cui è presente un test completo, quindi di installazione ed effettivo test, nella cartella test/test_engine_local che instanzia un oggetto MosaicEngine che fa da entry point di tutto e prende come input un "modello agronomico" che è, nella cartella models, agro_model.py. Inoltre permette l'installazione ondemand del package, tutto compreso nel file test/test_engine_cloud/engine_test.py.

### 2- Engine | Cloud
L'engine è possibile testarlo solamente previa installazione del package. Per cui è presente un test completo, quindi di installazione ed effettivo test, nella cartella test/test_engine_cloud che instanzia un oggetto MosaicEngine che fa da entry point di tutto e prende come input un "modello agronomico" che è, nella cartella models, agro_model.py.

Qui dobbiamo aggiungere del codice in quanto l'environment in cloud è diverso dal locale, quindi dobbiamo "ricreare l'ambiente cloud". Che comunque è tutto compreso nel file test/test_engine_cloud/engine_test.py

### Known bugs
- Spesso quando lanciamo il test, che sia in local o cloud abbiamo bisogno di rilanciarlo due volte, in quanto il building ogni tanto fallisce non installando correttamente il package.
- Lanciare il comando python3 engine_test.py potrebbe risultare in errore, aggiungere sudo.

### 3- Rules
Rules can be tested as it follows:
- Install package pytest and unittest
- Get to mosaic-framework folder (main one)
- run command: 
```
python3 -m pytest tests/ -v
```

#### 3.1 - Create a new unittest for a Rule
- Go to unittests folder
- Define a new module: NAME_OF_THE_RULE_test.py (the _test is fundamental)
- Write the test, recalling that: 
    -   Each class must start with "Test"
    -   Each function must start with "test"
- Do not forget to include at the end:
```
if __name__ == '__main__':
    unittest.main()
```

## TODO
- NonRegression Tests
- Logging 
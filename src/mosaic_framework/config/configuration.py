################################################################################
# Module:      configuration.py
# Description: Gather configuration for various components.
# Author:      Stefano Zimmitti
# Date:        16/05/2024
# Company:     xFarm Technologies
################################################################################


#Components configurations
METADATA = {
        "metadata":{
            "allowed-extra-params": True
        },
        "params":{
            "label":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "Function::fill_label_source"
            },
            "tags":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : []
            }
    }
}

SOURCE = {
        "metadata":{
            "allowed-extra-params": False
        },
        "params":{
            "environment":{
                "values"  : ["cloud", "local", "default"], 
                "optional": True, 
                "default" :"local"
            },
            "label":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "Function::fill_label_source"
            },
            "file":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : "Function::skip_autofill"
            },
            "columns":{
                "values"  : ["Any"],
                "optional": True,
                "default" : "auto"
            }
    }
}

GEOSPATIAL_SOURCE = {
        "metadata":{
            "allowed-extra-params": False
        },
        "params":{
            "label":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "Function::fill_label_source"
            },
            "latitude":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : ""
            },
            "longitude":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : ""
            }
    }
}

COLTURE = {
        "metadata":{
            "allowed-extra-params": True
        },
        "params":{
            "label":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "Function::fill_label_source"
            },
            "commodity_id":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : None
            },
            "destination_use_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            },
            "precocity_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            },
            "planting_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            },
            "calendar_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : 0
            },
            "policy_type_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            },
            "destination_use_id":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            },
            "model_type":{
                "values"  : ["gdd", "fixed", "policy", "skip"], 
                "optional": True, 
                "default" : "skip"
            },
            "data_source":{
                "values"  : ["api", "local"], 
                "optional": True, 
                "default" : "api"
            },
            "start_date":{
                "values"  : ["Any"],
                "optional": True, 
                "default" : None
            },
            "susceptibility":{
                "values"  : ["Any"], 
                "optional": True, 
                "default" : None
            }
    }
}

SUSCEPTIBILITY = {
    "metadata":{
        "allowed-extra-params": True
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": True, 
            "default" : "Function::fill_label_source"
        },
        "variety_id":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "disease_id":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "mapping":{
            "values"  : ["Any"], 
            "optional": True,
            "default" : {'S':1,'D':0,'R':-1} 
        }
    }
}

MODEL = {
    "metadata":{
        "allowed-extra-params": True
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : "Function::fill_label_source"
        },
        "outputs":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "history":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "windowing":{
            "values"  : ['Any'],
            "optional": True,
            "default" : "default"
        },
        "days":{
            "values"  : ["Any"], 
            "optional": True, 
            "default" : "default"
        },
        "date_column":{
            "values"  : ["Any"], 
            "optional": True, 
            "default" : "sampleDate"
        },
        "granularity":{
            "values"  : ['hourly', 'daily'],
            "optional": False,
            "default" : "default"
        },
        "debug": {
            "values"  : ['active', 'default'],
            "optional": True,
            "default" : "default"
        }
    },
    "data":{
        "date_column": ["sampledate", "sample_date", "date", "datetime", "time"],
        "rules_hub":{
            "params_to_include":['column', 'is_implicit']
        }
    }
}

DATA_BRIDGE = {
    "metadata":{
        "allowed-extra-params": False
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },

        "connect_in":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "connect_out":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        }
    }
}

VALIDATOR = {
    "metadata":{
        "allowed-extra-params": False
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "source":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "activity":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "report":{
            "values"  : [True, False], 
            "optional": True, 
            "default" : False
        }
    }
}

MODEL_VALIDATION_ACTIVITY = {
    "metadata":{
        "allowed-extra-params": False
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": True, 
            "default" : "Function::fill_label_source"
        },
        "process":{
            "values"  : ["simple", "deep", "default"], 
            "optional": False, 
            "default" : ""
        }
    }
}

LAMBDA_OUTPUT = {
    "metadata":{
        "allowed-extra-params": False
    },
    "params":{
        "label":{
            "values"  : ["Any"], 
            "optional": False, 
            "default" : ""
        },
        "output_format":{
            "values"  : ["compact", "detailed", "default"], 
            "optional": True, 
            "default" : "default"
        }
    }
}

CUSTOM_WINDOW = {
    "metadata":{
            "allowed-extra-params": True
        },
        "params":{
            "risk":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "default"
            },
            "prevision":{
                "values"  : ["Any"], 
                "optional": False, 
                "default" : "default"
            }
        }
}

#Readers and writers
WRITERS_MAPPING = {
    "txt"  : "TextWriter",
    "json" : "JsonWriter",
    "csv"  : "CsvWriter",
    "xlsx" : "ExcelWriter",
    "py"   : "PyWriter"
}

READERS_MAPPING = {
    "txt"  : "TextReader",
    "json" : "JsonReader",
    "csv"  : "CsvReader",
    "xlsx" : "ExcelReader",
    "py"   : "PyReader"
}

CONVERTERS_MAPPING = {
    "extensions":{
        "json"  : "json",
        "csv"   : "dataframe",
        "xlsx"  : "xlsx"
    },
    "types"     :{
        "pandas.core.frame.DataFrame": "dataframe",
        "dataframe"                  : "dataframe",
        "dict"                       : "json",
        "list"                       : "json"
    }
}
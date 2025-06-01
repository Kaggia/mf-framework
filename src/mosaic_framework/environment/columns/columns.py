################################################################################
# Module:      columns.py
# Description: Collection of columns that can be detected or assigned.
# Author:      Stefano Zimmitti
# Date:        26/08/2024
# Company:     xFarm Technologies
################################################################################

from mosaic_framework.environment.columns.column import Column

class GenericColumn(Column):
    """
    Generic column used as fallback when Levenshtein algo detect
    an high number of similar columns.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['generic_column']

class SampleDate(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name       = name
        self.other_names = ['sampledate', 'date', 'datetime']

class Temperature(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['temp', 'temperature', 'datetime']

class Humidity(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['hum', 'humidity']

class MinimumTemperature(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['mintemp', 'min_temp', 'minTemp']

class MaximumTemperature(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['maxtemp', 'max_temp', 'maxTemp']

class AverageTemperature(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['avgtemp', 'avg_temp', 'avgTemp']

class MinimumHumidity(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['minhum', 'min_hum', 'minHum']

class MaximumHumidity(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['maxhum', 'max_hum', 'maxHum']

class AverageHumidity(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['avghum', 'avg_hum', 'avgHum', 'avgHumidity']

class Rain(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['precipitation']

class LeafWetness(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['leafw', 'lw']

class IsDaylight(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['isdaylight', 'daylight', 'is_daylight']

class DataSource(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['datasource', 'data_source']

class GrowingDegreeDays(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['gdd', 'GDD', 'growing_degree_days', 'growingdegreedays']

class CumulatedGrowingDegreeDays(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['cumulated_gdd', 'cum_gdd', 'CUM_GDD', 'cumulated_growing_degree_days', 'cumulatedgrowingdegreedays']

class GrowingDegreeHourly(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['gdh', 'GDH', 'growing_degree_hourly', 'growingdegreehourly']

class CumulatedGrowingDegreeHourly(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['cumulated_gdh', 'cum_gdh', 'CUM_GDH', 'cumulated_growing_degree_hourly', 'cumulatedgrowingdegreedays']

class WindSpeed(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['wind_speed', 'windspeed']

class WindDirection(Column):
    """
    Class used to map a specified type of column.

    Args:
        name (str): name of the column.
    """

    def __init__(self, name:str):
        self.name = name
        self.other_names = ['wind_direction', 'winddirection', 'wind_dir']

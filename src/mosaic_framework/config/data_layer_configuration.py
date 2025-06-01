################################################################################
# Module:      data_layer_configuration.py
# Description: Gather configuration for data layer components.
# Author:      Stefano Zimmitti
# Date:        01/08/2024
# Company:     xFarm Technologies
################################################################################

PHENOSTAGE_API_URL     = "https://growth-models.agrord.xfarm.ag/{stage}/commodities/{commodity_id}/phenostages?destination_use_id={destination_use_id}&precocity_id={precocity_id}&start_date={start_date}&model_type={model_type}"

MADE_API_URL_V1        = "https://api-xmade.xfarm.ag/api/v1/xmade/getWeatherData?lat={lat}&lng={lon}&historical_req_type={historical_1granularity}&historical_interval={data_points_data_type_plural}"

SUSCEPTIBILITY_API_URL = "https://disease.agrord.xfarm.ag/{stage}/api/private/v1/get_susceptibility/commodity_id/{commodity_id}/variety_id/{variety_id}/disease_id/{disease_id}"

INSURTECH_API_URL      = "https://insurtech.agrord.xfarm.ag/{stage}/api/private/v1/get_policy_domain?commodity_id={commodity_id}&policy_type_id={policy_type_id}&precocity_id={precocity_id}&calendar_id={calendar_id}&planting_id={planting_id}&destination_use_id={destination_use_id}&disease_id={disease_id}"
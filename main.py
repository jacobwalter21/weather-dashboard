# API Source: https://open-meteo.com/en/docs/historical-weather-api
# Some of the below code is copied from the above source, which provides detailed examples on how to call the API
# It has been modified to collect information on a list of cities with Lat/Longs provided

import openmeteo_requests
import argparse
import requests_cache
import pandas as pd
from retry_requests import retry
from mongo_connection import mongo_connection

def main(args):
    # Load csv with US capitals and Lat/Longs
    locations_df = pd.read_csv(args.cities)

    # Inputs to API Call
    daily_params = [
        "temperature_2m_max", 
        "temperature_2m_min", 
        "temperature_2m_mean", 
        "daylight_duration", 
        "sunshine_duration", 
        "precipitation_sum", 
        "rain_sum", 
        "snowfall_sum", 
        "wind_speed_10m_max", 
        "wind_gusts_10m_max", 
        "wind_direction_10m_dominant"
    ]

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Connect to API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": locations_df.latitude.to_list(),
        "longitude": locations_df.longitude.to_list(),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "daily": daily_params,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/New_York"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process each location
    all_df = pd.DataFrame()
    for idx, response in enumerate(responses):
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        
        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        
        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}
        
        for didx, param in enumerate(daily_params):
            daily_data[param] = daily.Variables(didx).ValuesAsNumpy()
        
        daily_dataframe = pd.DataFrame(data = daily_data)
        
        daily_dataframe["City"] = locations_df["description"][idx]
        daily_dataframe["State"] = locations_df["name"][idx]
        
        daily_dataframe["Latitude"] = locations_df["latitude"][idx]
        daily_dataframe["Longitude"] = locations_df["longitude"][idx]
        
        if all_df.empty:
            all_df = daily_dataframe
        else:
            all_df = pd.concat([all_df, daily_dataframe])

    # Save a Copy to CSV
    all_df.to_csv("weather_data.csv")

    # Write to Mongo DB
    mc = mongo_connection("config.yaml")
    mc.establish_connection()
    mc.write_to_mongo(all_df)

if __name__ == "__main__":
    """
    Extracts Historical Weather Data for US Captial Cities from Open-Meteo API and writes to Mongo DB
        https://open-meteo.com/en/docs/historical-weather-api
    Args:
        --start-date: Beginning of date range to collect weather data
        --end-date: End of date range to collect weather data
        --cities: csv file containing Latitude/Longitude information
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-s','--start-date', 
                        help="Beginning of date range to collect weather data (e.g. 1999-01-01)", 
                        required=True)
    parser.add_argument('-e','--end-date', 
                        help="End of date range to collect weather data (e.g. 1999-12-31)", 
                        required=True)
    parser.add_argument('-c','--cities',
                        help="csv file with cities to collect weather data (Lat/Longs required)",
                        default="us-state-capitals.csv",
                        required=False)
    args = parser.parse_args()

    main(args)

""" Gets weather data from NOAA APi, cleans data and stores locally """
import json
import configparser
import pandas as pd
from pandas.io.json import json_normalize
import requests


def get_weather_data(datatypes, years, token):
    """
    Loops through two lists containing weather data types
    and years to complete a string and make request to NOAA's API.

    Arguments
    ---------
    datatypes: list
        Contains list of weather datatypes in standard US measurements

    years: list
        Range of list

    token: string
        secret token provided via NOAA's website

    Returns
    -------
    List
        List of Pandas data frames containing api response data
    """
    try:
        list_df = []

        for datatype in datatypes:
            for year in years:

                string = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?" \
                        f"datasetid=GHCND&datatypeid={datatype}&limit=1000&" \
                        "stationid=GHCND:USW00094728&" \
                        f"startdate={year}-01-01&enddate={year}-12-31&" \
                        "units=standard&" \
                        "includemetadata=False"

                response = requests.get(string, headers={'token': token})
                weather_data = json.loads(response.text)
                weather_df = json_normalize(weather_data, 'results')
                list_df.append(weather_df)

        return list_df

    except requests.RequestException as exception:
        return exception


def clean_weather_data(list_of_dfs):
    """
    Concatenates list of dataframes.
    Cleans up data column to only reflect data, no time values.
    Drops the station and attribute columns.
    Pivot dataframe to make weather datatype each a column.

    Arguments
    ---------
    list_of_dfs: list
        Contains list of dataframes containing weather data from API response

    Returns
    -------
    Pandas DataFrame
        A clean concatenated pandas dataframe
    """

    total_df = pd.concat(list_of_dfs)
    total_df["date"] = pd.to_datetime(total_df["date"], format='%Y/%m/%d')
    total_df = total_df.drop(columns=['station', 'attributes'])
    total_df = total_df.pivot(index="date", columns="datatype", values="value")

    return total_df


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('tokens.ini')
    noaa_token = config.get('WEATHER', 'Token')

    weather_types = ["TMIN", "TMAX", "PRCP", "SNOW", "SNWD"]
    req_years = list(range(2014, 2019))

    lista_df = get_weather_data(weather_types, req_years, noaa_token)

    final_df = clean_weather_data(lista_df)

    final_df.to_json("data/weather_data.json", orient="index")

# data processing
import pandas as pd

# asynchronicity
import asyncio
import aiohttp

# time
import time

# browser automation for scraping
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# functions typing
from typing import Dict, Tuple

### TOP BOTTOM FUNCTION DEFINITION
async def get_endpoint_data(endpoint: str, communities: Dict[str, str]) -> pd.DataFrame:
    """
    Fetches and processes data from Red Eléctrica Española (REE) endpoint for a 
    given endpoint from their API and a given dictionary of community geo_ids.
    The function is set to a range of years from 2019 to 2021.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        communities (Dict[str, str]): A dictionary where keys are community names and values are their codes.

    Returns:
        pd.DataFrame: Concatenated DataFrame with data from all processed years and communities.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_process(year,ccaa,cod_ccaa, endpoint, session) for year in range(2019,2022) for ccaa, cod_ccaa in communities.items()]
        results = await asyncio.gather(*tasks)

    final_df = pd.concat(results, ignore_index=True)
    return final_df


async def fetch_and_process(year: int, ccaa: str, cod_ccaa: str, endpoint: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    """
    Fetches data for a specific year and community, processes it, and returns it as a DataFrame.

    Args:
        year (int): Year for which data is fetched.
        ccaa (str): Community name.
        cod_ccaa (str): Community code.
        endpoint (str): The API endpoint to fetch data from.
        session (aiohttp.ClientSession): Asynchronous session for HTTP requests.

    Returns:
        pd.DataFrame: DataFrame containing processed data for the given year and community.
    """
    url = get_month_url(year,cod_ccaa,endpoint)
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error {response.status} en la URL: {url}")
            return pd.DataFrame() 
        try:
            data = await response.json()  
        except aiohttp.ContentTypeError:
            print(f"Error en la respuesta JSON para la URL: {url}")
            return pd.DataFrame() 

        if endpoint == "generacion/estructura-renovables":
            values_dict_list_total = list()
            for type_i in data['included']:
                values_dict_list = type_i['attributes']['values']
                for value_dict in values_dict_list:
                    value_dict["type"] = type_i["type"]
                
                values_dict_list_total.extend(values_dict_list)
        else:
            values_dict_list_total = data['included'][0]['attributes']['values']
            
        headers = list(values_dict_list_total[0].keys())
        headers.extend(["ccaa","cod_ccaa"])

        year_month_df = pd.DataFrame(
            [tuple([*value_dict.values(),ccaa,cod_ccaa]) for value_dict in values_dict_list_total],
            columns=headers
        )

    return year_month_df



def get_month_url(year: int, geo_id: str, endpoint: str) -> str:
    """
    Constructs the URL to fetch data for a specified year and geographic ID.

    Args:
        year (int): Year for the data.
        geo_id (str): Geographic ID code.
        endpoint (str): API endpoint indicating the data type.

    Returns:
        str: Formatted URL string for the API request.
    """
    if not endpoint == "generacion/estructura-renovables":
        endpoint = "demanda/evolucion"

    year_url = f"https://apidatos.ree.es/es/datos/{endpoint}?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=month&geo_limit=ccaa&geo_ids={geo_id}"

    return year_url


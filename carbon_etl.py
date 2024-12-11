import pandas as pd 
import json
import logging
import requests
from datetime import datetime 

def get_carbon_data():    
    try:
        headers = {
            'Accept': 'application/json'
        }
            
        r = requests.get('https://api.carbonintensity.org.uk/generation',
                        params={},
                        headers = headers,
                        timeout=10
                    )

        response = r.json()["data"][0]["generationmix"]

        return response
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)

def run_carbon_etl():
    carbon_results = get_carbon_data()
    list = []

    for carbon in carbon_results:
        normalized_carbon_data = {
            "fuel_type": carbon["fuel"],
            "percentage": carbon["percentage"]
        }

        list.append(normalized_carbon_data)

    df = pd.DataFrame(list)
    df.to_csv('s3://makdatabucket/normalized_carbon_data.csv')

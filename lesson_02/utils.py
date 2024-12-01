import os
import requests
import json

# Function for acquiring data from the API
def fetch_sales_data(date, page, auth_token):
    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': date, 'page': page},
        headers={'Authorization': auth_token},
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")
    
    return response.json()

# Function for writing data to a JSON file
def save_to_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

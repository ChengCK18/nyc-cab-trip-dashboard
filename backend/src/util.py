import requests
import os
from flask import jsonify

def u_download_file(year_of_interest,month_of_interest):
    file_of_interest = f'yellow_tripdata_{year_of_interest}-{month_of_interest}.parquet'
    save_path = 'data/'+file_of_interest
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_of_interest}'

    if not os.path.exists(save_path): # Download the parquet file if it does not exist
        response = requests.get(url)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
        else:
            return None

    return save_path


 


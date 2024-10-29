import requests
import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# API and Database setup
gill_api = os.getenv('GILL_API_KEY')
url = "https://api.giinfotech.ae/api/Hotel/DestinationInfo"

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

gill_table = 'gill_hotel_info_table'

# Function to get distinct city names
def only_column_info(table, column, engine):
    query = f"SELECT DISTINCT {column} FROM {table};"
    df = pd.read_sql(query, engine)
    return df[column].tolist()

# Function to update `GiDestinationId` for each city
def update_gi_destination_id():
    city_name_data = only_column_info(table=gill_table, column='CityName', engine=engine)
    
    for city_name in city_name_data:
        # Prepare payload with the city name
        payload = json.dumps({
            "destination": f"{city_name}"
        })
        headers = {
            'ApiKey': f'{gill_api}',
            'Content-Type': 'application/json'
        }
        
        # Make API request
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()
        
        # Check if API call was successful and extract `giDestinationId`
        if response_data.get("isSuccess") and response_data.get("data"):
            gi_destination_id = response_data["data"][0]["giDestinationId"]
            
            # Update database with `giDestinationId`
            update_query = text(f"""
                UPDATE {gill_table}
                SET GiDestinationId = :gi_destination_id
                WHERE CityName = :city_name
            """)
            with engine.connect() as conn:
                conn.execute(update_query, {'gi_destination_id': gi_destination_id, 'city_name': city_name})
                print(f"Successfully updated GiDestinationId for city '{city_name}' to {gi_destination_id}.")
        else:
            print(f"Failed to retrieve GiDestinationId for city '{city_name}'. Response: {response_data}")

# Run the update function
update_gi_destination_id()

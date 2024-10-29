from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import json
import requests

# Load environment variables
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

# Get distinct city names
def fetch_city_names(table, column, engine):
    query = f"SELECT DISTINCT {column} FROM {table};"
    return pd.read_sql(query, engine)[column].tolist()

# Bulk update `GiDestinationId`
def bulk_update_gi_destination_id():
    city_names = fetch_city_names(table=gill_table, column='CityName', engine=engine)

    # Prepare requests in bulk
    data_updates = []
    headers = {'ApiKey': f'{gill_api}', 'Content-Type': 'application/json'}
    
    for city in city_names:
        payload = json.dumps({"destination": city})
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()
        
        # If successful, prepare update entry
        if response_data.get("isSuccess") and response_data.get("data"):
            gi_destination_id = response_data["data"][0]["giDestinationId"]
            data_updates.append({'gi_destination_id': gi_destination_id, 'city_name': city})

    # Execute bulk update in a single transaction for better performance
    if data_updates:
        with engine.connect() as conn:
            conn.execute(
                text(f"""
                    UPDATE {gill_table}
                    SET GiDestinationId = :gi_destination_id
                    WHERE CityName = :city_name
                """),
                data_updates
            )

# Run the optimized update function
bulk_update_gi_destination_id()

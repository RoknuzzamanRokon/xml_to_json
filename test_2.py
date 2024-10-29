from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import json
import aiohttp
import asyncio

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

async def fetch_gi_destination_id(session, city):
    """Fetch GiDestinationId for a given city."""
    payload = json.dumps({"destination": city})
    headers = {'ApiKey': f'{gill_api}', 'Content-Type': 'application/json'}
    
    async with session.post(url, headers=headers, data=payload) as response:
        if response.status == 200:
            response_data = await response.json()
            if response_data.get("isSuccess") and response_data.get("data"):
                return response_data["data"][0]["giDestinationId"]
    return None

async def bulk_update_gi_destination_id():
    """Bulk update GiDestinationId using asynchronous requests."""
    city_names = fetch_city_names(table=gill_table, column='CityName', engine=engine)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_gi_destination_id(session, city) for city in city_names]
        results = await asyncio.gather(*tasks)

    data_updates = [{'gi_destination_id': gi_id, 'city_name': city} 
                    for gi_id, city in zip(results, city_names) if gi_id is not None]

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

            # Fetch and display the updated data for the given cities
            for city in city_names:
                fetch_and_display_updated_data(conn, city)

def fetch_and_display_updated_data(conn, city):
    """Fetch and print the updated GiDestinationId for a specific city."""
    query = text(f"SELECT GiDestinationId, CityName FROM {gill_table} WHERE CityName = :city_name;")
    result = conn.execute(query, {'city_name': city}).fetchone()
    
    if result:
        print(f"Updated Data - City: {result['CityName']}, GiDestinationId: {result['GiDestinationId']}")
    else:
        print(f"No data found for City: {city}")

# Run the optimized update function
if __name__ == "__main__":
    asyncio.run(bulk_update_gi_destination_id())

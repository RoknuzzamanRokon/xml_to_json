from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import json
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import time


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

gill_table = 'hotels_info_with_gidestination_code'


semaphore = asyncio.Semaphore(5)  


def fetch_city_names(table, column, engine):
    query = f"SELECT DISTINCT {column} FROM {table};"
    return pd.read_sql(query, engine)[column].tolist()

async def fetch_gi_destination_id(session, city, retries=3):
    """Fetch GiDestinationId for a given city, with retries and concurrency control."""
    payload = json.dumps({"destination": city})
    headers = {'ApiKey': f'{gill_api}', 'Content-Type': 'application/json'}

    for attempt in range(retries):
        try:
            async with semaphore:  
                async with session.post(url, headers=headers, data=payload) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        if response_data.get("isSuccess") and response_data.get("data"):
                            return response_data["data"][0]["giDestinationId"]
            return None  
        except asyncio.TimeoutError:
            print(f"Timeout error for city '{city}', attempt {attempt + 1}/{retries}")
            if attempt == retries - 1:
                return None  


async def update_gi_destination_id(city, session):
    """Fetch and update GiDestinationId for a specific city."""
    gi_destination_id = await fetch_gi_destination_id(session, city)
    if gi_destination_id:
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    UPDATE {gill_table}
                    SET GiDestinationId = :gi_destination_id
                    WHERE CityName = :city_name
                """),
                {'gi_destination_id': gi_destination_id, 'city_name': city}
            )
            print(f"Update successful - City: {city}, GiDestinationId: {gi_destination_id}")

async def bulk_update_gi_destination_id():
    """Bulk update GiDestinationId using asynchronous requests with retries and concurrency limit."""
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%I:%M %p")  
    print(f"Start Time: {formatted_start_time}")

    city_names = fetch_city_names(table=gill_table, column='CityName', engine=engine)
    
    timeout = aiohttp.ClientTimeout(total=60)  
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [update_gi_destination_id(city, session) for city in city_names]
        await asyncio.gather(*tasks)

    
    end_time = time.time()  
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%I:%M %p")
    print(f"END time: {formatted_end_time}")
    total_time = end_time - start_time
    formatted_total_time = datetime.fromtimestamp(total_time).strftime("%I:%M %p")
    print(f"Total time taken for updates: ", formatted_total_time)


if __name__ == "__main__":
    asyncio.run(bulk_update_gi_destination_id())

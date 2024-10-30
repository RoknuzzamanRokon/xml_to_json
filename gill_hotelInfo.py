import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import time 
import aiohttp
import asyncio

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

gill_table = 'gill_hotel_info_table'
gill_api = os.getenv('GILL_API_KEY')


def only_column_info(table, column, engine):
    """Fetch distinct values from a specified column in a given table."""
    query = f"SELECT DISTINCT {column} FROM {table};"
    df = pd.read_sql(query, engine)
    return df[column].tolist()


async def fetch_hotel_info_by_systemId(session, systemId):
    """Fetch hotel information by system ID."""
    url = "https://api.giinfotech.ae/api/Hotel/HotelInfo"
    payload = json.dumps({"hotelCode": str(systemId)})
    headers = {
        'ApiKey': gill_api,
        'Content-Type': 'application/json'
    }
    
    try:
        async with session.post(url, headers=headers, data=payload) as response:
            if response.status == 200:
                response_data = await response.json()
                if response_data["isSuccess"]:
                    hotel_info = response_data["hotelInformation"]
                    hotel_info_json_data = json.dumps(hotel_info)

                    update_hotel_info(systemId=systemId, hotel_info_json_data=hotel_info_json_data, engine=engine)
                    print(f"Update hotel information for systemiD: {systemId}")

                    return hotel_info
                 
                else:
                    print(f"No hotel information found for systemID: {systemId}")
            else:
                print(f"Failed to fetch data for systemID {systemId}: {response.status}")

    except Exception as e:
        print(f"Error fetching data for system ID {systemId}: {e}")
    
    return []



def update_hotel_info(systemId, hotel_info_json_data, engine):
    query = text("""
        UPDATE gill_hotel_info_table
        SET HotelInfo = :hotel_info
        WHERE systemId = :systemId
        """)
    
    with engine.connect() as connection:
        connection.execute(query, {
            "hotel_info": hotel_info_json_data,
            "systemId": systemId
        })


async def main():
    start_time = time.time()
    print(f"Start Time: {start_time}")

    table = 'gill_hotel_info_table'
    system_ids = only_column_info(table=table, column='SystemId', engine=engine)

    async with aiohttp.ClientSession() as session:
        for systemId in system_ids:
            hotel_info = await fetch_hotel_info_by_systemId(session=session, systemId=systemId)
            if hotel_info:
                update_hotel_info(systemId, json.dumps(hotel_info), engine)
                print(f"Updated HotelInfo for SystemId: {systemId}")

    end_time = time.time()
    print(f"END time: {end_time}")
    total_time = end_time -start_time
    print(f"Total time take for updates: {total_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
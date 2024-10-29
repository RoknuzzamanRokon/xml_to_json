import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import time  # To measure execution time
import aiohttp
import asyncio

load_dotenv()

# Database connection details
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

async def fetch_hotels_by_destination_id(session, destination_id):
    """Fetch hotel information for a given giDestinationId."""
    url = "https://api.giinfotech.ae/api/Hotel/HotelsInfoByDestinationId"
    payload = json.dumps({"destinationCode": str(destination_id)})
    headers = {
        'ApiKey': gill_api,
        'Content-Type': 'application/json'
    }

    try:
        async with session.post(url, headers=headers, data=payload) as response:
            if response.status == 200:
                response_data = await response.json()
                if response_data["isSuccess"]:
                    return response_data["hotelsInformation"]
                else:
                    print(f"No hotel information found for destination ID: {destination_id}")
            else:
                print(f"Failed to fetch data for destination ID {destination_id}: {response.status}")
    except Exception as e:
        print(f"Error fetching data for destination ID {destination_id}: {e}")
    return []

def insert_hotels_into_db(hotels):
    """Insert hotel information into the database while preserving specific existing values."""
    with engine.connect() as connection:
        for hotel in hotels:
            if hotel is None:  # Skip if hotel data is null
                print("Hotel data is null, skipping...")
                continue
            
            # Ensure all necessary fields are present before insertion
            required_fields = ['giDestinationId', 'name', 'systemId', 'rating', 'address1', 'address2', 'imageUrl', 'geoCode']
            if not all(field in hotel for field in required_fields):
                print(f"Incomplete hotel data for insertion: {hotel}")
                continue
            
            query = text("""
                INSERT INTO gill_hotel_info_table (
                    GiDestinationId, HotelName, SystemId, Rating, 
                    Address1, Address2, ImageUrl, HotelLatitude, HotelLongitude, 
                    CityName, CountryName, CountryCode, PostalCode
                )
                VALUES (
                    :GiDestinationId, :HotelName, :SystemId, :Rating, 
                    :Address1, :Address2, :ImageUrl, :HotelLatitude, :HotelLongitude, 
                    :CityName, :CountryName, :CountryCode, :PostalCode
                )
                ON DUPLICATE KEY UPDATE
                    HotelName = VALUES(HotelName),
                    SystemId = VALUES(SystemId),
                    Rating = VALUES(Rating),
                    Address1 = VALUES(Address1),
                    Address2 = VALUES(Address2),
                    ImageUrl = VALUES(ImageUrl),
                    HotelLatitude = VALUES(HotelLatitude),
                    HotelLongitude = VALUES(HotelLongitude)
                    -- Existing values will remain unchanged for CityName, CountryName, CountryCode, PostalCode, GiDestinationId
            """)
            connection.execute(query, {
                'GiDestinationId': hotel["giDestinationId"],
                'HotelName': hotel["name"],
                'SystemId': hotel["systemId"],
                'Rating': hotel["rating"],
                'Address1': hotel["address1"],
                'Address2': hotel["address2"],
                'ImageUrl': hotel["imageUrl"],
                'HotelLatitude': hotel["geoCode"]["lat"],
                'HotelLongitude': hotel["geoCode"]["lon"],
                'CityName': hotel["city"],          
                'CountryName': hotel.get("countryName", ""),  
                'CountryCode': hotel.get("countryCode", ""),  
                'PostalCode': hotel.get("postalCode", ""),    
            })

async def main():
    """Main function to coordinate the fetching and storing of hotel data."""
    start_time = time.time()  
    print(f"Start Time: {start_time}")

    destination_ids = only_column_info(gill_table, 'GiDestinationId', engine)
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_hotels_by_destination_id(session, destination_id) for destination_id in destination_ids]
        results = await asyncio.gather(*tasks)

        for index, hotels in enumerate(results, start=1):
            if hotels:  # Proceed only if hotels data is not empty
                insert_hotels_into_db(hotels)
                print(index)

    end_time = time.time()  
    print(f"END time: {end_time}")
    total_time = end_time - start_time
    print(f"Total time taken for updates: {total_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())

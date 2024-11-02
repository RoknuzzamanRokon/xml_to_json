import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import time  
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

gill_table = 'hotels_info_with_gidestination_code'
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
    with engine.begin() as connection:
        for hotel in hotels:
            if hotel is None:  
                print("Hotel data is null, skipping...")
                continue
            
            # Ensure all necessary fields are present before insertion
            required_fields = ['giDestinationId', 'name', 'systemId', 'rating', 'city', 'address1', 'address2', 'imageUrl', 'geoCode']
            if not all(field in hotel for field in required_fields):
                print(f"Incomplete hotel data for insertion: {hotel}")
                continue
            
            query = text("""
                INSERT INTO hotel_info_all (
                    GiDestinationId, HotelName, SystemId, Rating, City, 
                    Address1, Address2, ImageUrl, Latitude, Longitude
                    
                )
                VALUES (
                    :GiDestinationId, :HotelName, :SystemId, :Rating, :City,
                    :Address1, :Address2, :ImageUrl, :Latitude, :Longitude
                    
                )
                ON DUPLICATE KEY UPDATE
                    HotelName = VALUES(HotelName),
                    SystemId = VALUES(SystemId),
                    Rating = VALUES(Rating),
                    City = VALUES(City),
                    Address1 = VALUES(Address1),
                    Address2 = VALUES(Address2),
                    ImageUrl = VALUES(ImageUrl),
                    Latitude = VALUES(Latitude),
                    Longitude = VALUES(Longitude)
            """)


            connection.execute(query, {
                'GiDestinationId': hotel["giDestinationId"],
                'HotelName': hotel["name"],
                'SystemId': hotel["systemId"],
                'Rating': hotel["rating"],
                'City': hotel["city"],
                'Address1': hotel["address1"],
                'Address2': hotel["address2"],
                'ImageUrl': hotel["imageUrl"],
                'Latitude': hotel["geoCode"]["lat"],
                'Longitude': hotel["geoCode"]["lon"]
            })
            print(f"Update successful - GiDestinationId: {hotel['giDestinationId']}")

async def main():
    """Main function to coordinate the fetching and storing of hotel data."""
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%I:%M %p")  
    print(f"Start Time: {formatted_start_time}")

    destination_ids = only_column_info(gill_table, 'GiDestinationId', engine)
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_hotels_by_destination_id(session, destination_id) for destination_id in destination_ids]
        results = await asyncio.gather(*tasks)

        for index, hotels in enumerate(results, start=1):
            if hotels:
                insert_hotels_into_db(hotels)
                print(index)


    end_time = time.time()  
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%I:%M %p")
    print(f"END time: {formatted_end_time}")
    total_time = end_time - start_time
    formatted_total_time = datetime.fromtimestamp(total_time).strftime("%I:%M %p")
    print(f"Total time taken for updates: ", formatted_total_time)

if __name__ == "__main__":
    asyncio.run(main())

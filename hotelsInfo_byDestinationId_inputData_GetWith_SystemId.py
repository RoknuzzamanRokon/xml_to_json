import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import os
import pandas as pd
import time

# Load environment variables
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
    try:
        query = f"SELECT DISTINCT {column} FROM {table};"
        df = pd.read_sql(query, engine)
        return df[column].tolist()
    except Exception as e:
        print(f"Error fetching column info: {e}")
        return []


def fetch_hotels_by_destination_id(destination_id):
    """Fetch hotel information for a given giDestinationId using synchronous requests."""
    url = "https://api.giinfotech.ae/api/Hotel/HotelsInfoByDestinationId"
    payload = json.dumps({"destinationCode": str(destination_id)})
    headers = {
        'ApiKey': gill_api,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get("isSuccess", False):
                return response_data.get("hotelsInformation", []), "Done"
            else:
                print(f"No hotel information found for destination ID: {destination_id}")
                return [], "Cannot find."
        else:
            print(f"Failed to fetch data for destination ID {destination_id}: {response.status_code}")
            return [], "Cannot find."
    except Exception as e:
        print(f"Error fetching data for destination ID {destination_id}: {e}")
        return [], "Cannot find."

def insert_hotels_into_db(hotels, status_update):
    """Insert hotel information into the database while preserving specific existing values."""
    with engine.begin() as connection:
        if not hotels:
            # Handle the case where there are no hotel details to insert
            query = text("""
                INSERT INTO hotel_info_all (
                    GiDestinationId, StatusUpdate
                )
                VALUES (
                    :GiDestinationId, :StatusUpdate
                )
                ON DUPLICATE KEY UPDATE
                    StatusUpdate = VALUES(StatusUpdate)
            """)
            try:
                # Use a placeholder destination ID for "Cannot find" status
                connection.execute(query, {
                    'GiDestinationId': hotels.get("giDestinationId", ""),
                    'StatusUpdate': status_update
                })
                print(f"Update successful for missing data - GiDestinationId: {hotels.get('giDestinationId')}")
            except Exception as e:
                print(f"Error updating hotel data for missing info: {e}")
            return  # End function for "Cannot find" case

        # Process hotels with full data if available
        for hotel in hotels:
            if hotel is None:  
                print("Hotel data is null, skipping...")
                continue

            # Ensure all necessary fields are present before insertion
            required_fields = ['giDestinationId', 'name', 'systemId', 'rating', 'address1', 'address2', 'imageUrl', 'geoCode']
            if not all(field in hotel for field in required_fields):
                print(f"Incomplete hotel data for insertion: {hotel}")
                continue
            
            # Prepare and execute the insertion query
            query = text("""
                INSERT INTO hotel_info_all (
                    GiDestinationId, HotelName, SystemId, Rating, City, 
                    Address1, Address2, ImageUrl, Latitude, Longitude, StatusUpdate
                )
                VALUES (
                   :GiDestinationId, :HotelName, :SystemId, :Rating, :City,
                    :Address1, :Address2, :ImageUrl, :Latitude, :Longitude, :StatusUpdate
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
                    Longitude = VALUES(Longitude),
                    StatusUpdate = VALUES(StatusUpdate)
            """)
            try:
                connection.execute(query, {
                    'GiDestinationId': hotel.get("giDestinationId", ""),
                    'HotelName': hotel.get("name", ""),
                    'SystemId': hotel.get("systemId", ""),
                    'Rating': hotel.get("rating", ""),
                    'City': hotel.get("city", ""),
                    'Address1': hotel.get("address1", ""),
                    'Address2': hotel.get("address2", ""),
                    'ImageUrl': hotel.get("imageUrl", ""),
                    'Latitude': hotel.get("geoCode", {}).get("lat", None),
                    'Longitude': hotel.get("geoCode", {}).get("lon", None),
                    'StatusUpdate': status_update
                })
                print(f"Update successful - GiDestinationId: {hotel['giDestinationId']}")
            except Exception as e:
                print(f"Error updating hotel data: {e}")


def main():
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%I:%M %p")  
    print(f"Start Time: {formatted_start_time}")

    destination_ids = only_column_info(gill_table, 'GiDestinationId', engine)

    for index, destination_id in enumerate(destination_ids, start=1):
        hotels, status_update = fetch_hotels_by_destination_id(destination_id)
        if hotels or status_update == "Cannot find.":
            insert_hotels_into_db(hotels, status_update)
            print(f"Processed Destination ID {destination_id} ({index}/{len(destination_ids)})")

    end_time = time.time()  
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%I:%M %p")
    print(f"END time: {formatted_end_time}")
    total_time = end_time - start_time
    print(f"Total time taken for updates: {total_time:.2f} seconds")


if __name__ == "__main__":
    main()

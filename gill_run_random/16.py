import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

table = 'hotel_info_all'
gill_api = os.getenv('GILL_API_KEY')


def only_column_info(table, column, engine):
    """Fetch the last 10 distinct values from a specified column in a given table."""
    try:
        query = f"""
            SELECT {column}
            FROM {table} 
            WHERE StatusUpdateHotelInfo != 'Done Json' 
            AND StatusUpdateHotelInfo != 'Not found json' 
            OR StatusUpdateHotelInfo IS NULL
            LIMIT 3000 OFFSET 160000;  -- Fetch rows 100 to 110
        """
        # Execute the query and load the results into a DataFrame
        df = pd.read_sql(query, engine)
        
        # Return the last 10 values from the column, as they are now ordered descending
        return df[column].tolist()

    except Exception as e:
        print(f"Error fetching column info: {e}")
        return []


def fetch_hotel_info_by_systemId(systemId):
    """Fetch hotel information by system ID using synchronous requests."""
    url = "https://api.giinfotech.ae/api/Hotel/HotelInfo"
    payload = json.dumps({"hotelCode": str(systemId)})
    headers = {
        'ApiKey': gill_api,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            response_data = response.json()
            if response_data["isSuccess"]:
                hotel_info = response_data["hotelInformation"]
                if hotel_info:  
                    return hotel_info
                else:
                    print(f"No hotel information found for systemID: {systemId}")
            else:
                print(f"API response not successful for systemID: {systemId}")
        else:
            print(f"Failed to fetch data for systemID {systemId}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data for system ID {systemId}: {e}")
    
    return None 


def update_hotel_info(systemId, hotel_info_json_data, status_update, engine, max_retries=5, base_delay=1):
    """Update hotel information in the database with retry logic using exponential backoff."""
    
    if isinstance(hotel_info_json_data, str):
        hotel_info_json_data = json.loads(hotel_info_json_data)
    
    country_code = hotel_info_json_data.get("address", {}).get("countryCode")
    zip_code = hotel_info_json_data.get("address", {}).get("zipCode")
    country_name = hotel_info_json_data.get("address", {}).get("countryName")
    
    query = text("""
        UPDATE hotel_info_all
        SET HotelInfo = :HotelInfo,
            StatusUpdateHotelInfo = :StatusUpdateHotelInfo,
            CountryCode = :CountryCode,
            ZipCode = :ZipCode,
            CountryName = :CountryName
        WHERE SystemId = :SystemId
    """)
    
    attempt = 0
    while attempt < max_retries:
        try:
            with engine.begin() as connection:
                connection.execute(query, {
                    "HotelInfo": json.dumps(hotel_info_json_data),
                    "StatusUpdateHotelInfo": status_update,
                    "CountryCode": country_code,
                    "ZipCode": zip_code,
                    "CountryName": country_name,
                    "SystemId": systemId
                })
                print(f"Updated SystemId: {systemId} with Status: {status_update}.")
                return  
        except Exception as e:
            attempt += 1
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))  # Exponential backoff
                print(f"Attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All {max_retries} attempts failed. Error: {e}")
                return


def main():
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%I:%M %p")  
    print(f"Start Time: {formatted_start_time}")

    system_ids = only_column_info(table='hotel_info_all', column='SystemId', engine=engine)
    # print(system_ids)

    for index, systemId in enumerate(system_ids, start=1):
        print(f"Processing SystemId: {systemId}")
        hotel_info = fetch_hotel_info_by_systemId(systemId)
        # print(hotel_info)

        if hotel_info:
            status_update = "Done Json"
            update_hotel_info(systemId, json.dumps(hotel_info), status_update, engine, max_retries=5, base_delay=1)
            print(f"Update system Id Done: No:{index} ----- {systemId}")
        else:
            status_update = "Not found json"
            update_hotel_info(systemId, json.dumps({}), status_update, engine)
            print(f"Update system Not found json: No:{index} ----- {systemId}")

    end_time = time.time()  
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%I:%M %p")
    print(f"END time: {formatted_end_time}")

    total_time = end_time - start_time
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)

    print(f"Total time taken for updates: {hours} hours, {minutes} minutes, {seconds} seconds")


if __name__ == "__main__":
    main()

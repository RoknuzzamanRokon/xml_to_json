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
    """Fetch distinct values from a specified column in a given table."""
    try:
        query = f"SELECT {column} FROM {table} WHERE StatusUpdateHotelInfo != 'Done Json' OR StatusUpdateHotelInfo IS NULL;"
        df = pd.read_sql(query, engine)
        return df[column].tolist()
    except Exception as e:
        print(f"Error fetching column info: {e}")
        return []


def only_select_column_info(table, column, country_code, engine):
    """Fetch distinct values from a specified column in a given table."""
    try:
        query = f"SELECT {column} FROM {table} WHERE StatusUpdateHotelInfo != 'Done Json' OR StatusUpdateHotelInfo IS NULL AND CountryCode = '{country_code}';"
        df = pd.read_sql(query, engine)
        

        unique_values = df[column].unique()
        print(len(unique_values))
        
        # len_list = len(list)
        # print(len_list)

        # all_values = df[column].tolist()
        
        # Count unique values
        unique_values = df[column].unique()
        # print(f"Number of unique values: {len(unique_values)}")
        
        # # Find duplicates
        # duplicates = df[df[column].duplicated()][column].tolist()  
        # duplicate_counts = df[column].value_counts()[df[column].value_counts() > 1]  

        # print(f"Total values: {len(all_values)}")
        # print(f"Duplicate values: {duplicates}")
        # print(f"Duplicate counts:\n{duplicate_counts}")

        return unique_values
    except Exception as e:
        print(f"Error fetching column info: {e}")
        return [], pd.Series()
        
data = only_select_column_info('hotel_info_all', 'SystemId', 'AE', engine)




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
                return hotel_info
            else:
                print(f"No hotel information found for systemID: {systemId}")
        else:
            print(f"Failed to fetch data for systemID {systemId}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data for system ID {systemId}: {e}")
    
    return []


def update_hotel_info(systemId, hotel_info_json_data, status_update, engine):
    """Update hotel information in the database."""

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


def main():
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%I:%M %p")  
    print(f"Start Time: {formatted_start_time}")

    system_ids = only_column_info(table='hotel_info_all', column='SystemId', engine=engine)

    system_ids = only_select_column_info('hotel_info_all', 'SystemId', 'AE', engine)

    for index, systemId in enumerate(system_ids, start=1):
        hotel_info = fetch_hotel_info_by_systemId(systemId)
        if hotel_info:
            status_update = "Done Json"
            update_hotel_info(systemId, json.dumps(hotel_info), status_update, engine)
            print(f"Update system Id Done: No:{index} ----- {systemId}")
        else:
            status_update = "Not found json"
            update_hotel_info(systemId, json.dumps({}), status_update, engine)
            print(f"Update system Not found json: No:{index} ---------------------------------------------------------------- {systemId}")

    end_time = time.time()  
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%I:%M %p")
    print(f"END time: {formatted_end_time}")

    total_time = end_time - start_time

    # print(f"Total time taken for updates: {total_time:.2f} seconds")
    # Convert total time to hours, minutes, and seconds

    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)

    print(f"Total time taken for updates: {hours} hours, {minutes} minutes, {seconds} seconds")


if __name__ == "__main__":
    main()




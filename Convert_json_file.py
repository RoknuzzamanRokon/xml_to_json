from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json
import os
import time


load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')



DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

table = 'hotel_info_all'


def get_system_id_list(table, column, engine):
    try: 
        query = f"SELECT {column} FROM {table} WHERE StatusUpdateHotelInfo = 'Done Json';"
        df = pd.read_sql(query, engine)
        # data_all = df[column].tolist()
        # print(len(data_all))
        data = list(set(df[column].tolist()))
        # print(len(data))
        return data
    except Exception as e:
        print(f"Error fetching column info: {e}")



def get_specifiq_data_from_system_id(table, systemid, engine):
    # SQL query to fetch data for a specific SystemId
    query = f"SELECT * FROM {table} WHERE SystemId = '{systemid}';"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found for the provided SystemId.")
        return None

    # Assuming only one row will be returned for a specific SystemId
    hotel_data = df.iloc[0].to_dict()

    # Extract nested JSON from the 'HotelInfo' field
    hotel_info = json.loads(hotel_data.get("HotelInfo", "{}"))
    
    # Construct the hotel photo data in the desired format
    hotel_photo_data = [
        {
            "picture_id": "NULL",  
            "title": "NULL",       
            "url": url             
        } for url in hotel_info.get("imageUrls", []) or []
    ]

    hotel_room_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": "NULL"
        } for ameList in hotel_info.get("masterRoomAmenities", []) or []
    ]
    
    hotel_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": "NULL"
        } for ameList in hotel_info.get("masterHotelAmenities", []) or []
    ]
    
    specific_data = {
        "hotel_id": hotel_data.get("SystemId", "NULL"),
        "name": hotel_info.get("name", hotel_data.get("HotelName", "NULL")),
        "name_local": hotel_info.get("name", hotel_data.get("HotelName", "NULL")),
        "hotel_formerly_name": "NULL",
        "destination_code": hotel_data.get("GiDestinationId", "NULL"),
        "country_code":  hotel_data.get("CountryCode", "NULL"),
        "brand_text": "NULL",
        "property_type": "NULL",
        "star_rating": hotel_info.get("rating", hotel_data.get("Rating", "NULL")),
        "chain": "NULL",
        "brand": "NULL",
        "logo": "NULL",
        "primary_photo": hotel_info.get("imageUrl", hotel_data.get("ImageUrl", "NULL")),
        "review_rating": {
            "source": "NULL",
            "number_of_reviews": "NULL",
            "rating_average": hotel_info.get("tripAdvisorRating", "NULL"),
            "popularity_score": "NULL",
        },
        "policies": {
            "checkin": {
                "begin_time": "NULL",
                "end_time": "NULL",
                "instructions": "NULL",
                "min_age": "NULL",
            },
            "checkout": {
                "time": "NULL",
            },
            "fees": {
                "optional": "NULL",
            },
            "know_before_you_go": "NULL",
            "pets": "NULL",
            "remark": "NULL",
            "child_and_extra_bed_policy": {
                "infant_age": "NULL",
                "children_age_from": "NULL",
                "children_age_to": "NULL",
                "children_stay_free": "NULL",
                "min_guest_age": "NULL"
            },
            "nationality_restrictions": "NULL",
        },
        "address": {
            "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
            "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
            "address_line_1": hotel_data.get("Address1", "NULL"),
            "address_line_2": hotel_data.get("Address2", "NULL"),
            "city": hotel_data.get("City", "NULL"),
            "state": hotel_info.get("address", {}).get("stateName", "NULL"),
            "country": hotel_data.get("CountryName", "NULL"),
            "country_code": hotel_data.get("CountryCode", "NULL"),
            "postal_code": hotel_data.get("ZipCode", "NULL"),
            "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}",
            "google_map_site_link": "NULL",
            "local_lang": {
                "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
                "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
                "address_line_1": hotel_data.get("Address1", "NULL"),
                "address_line_2": hotel_data.get("Address2", "NULL"),
                "city": hotel_data.get("City", "NULL"),
                "state": hotel_info.get("address", {}).get("stateName", "NULL"),
                "country": hotel_data.get("CountryName", "NULL"),
                "country_code": hotel_data.get("CountryCode", "NULL"),
                "postal_code": hotel_data.get("ZipCode", "NULL"),
                "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}", 
                "google_map_site_link": "NULL",
            },
            "mapping": {
                "continent_id": "NULL",
                "country_id": hotel_data.get("CountryCode", "NULL"),
                "province_id": "NULL",
                "state_id": "NULL",
                "city_id": "NULL",
                "area_id": "NULL"
            }
        },
        "contacts": {
            "phone_numbers": [hotel_info.get("contact", {}).get("phoneNo", "NULL")],
            "fax": hotel_info.get("contact", {}).get("faxNo", "NULL"),
            "email_address": "NULL",
            "website": hotel_info.get("contact", {}).get("website", hotel_data.get("Website", "NULL"))
        },
        "descriptions": [
            {
                "title": "NULL",
                "text": "NULL"
            }
        ],
        "room_type": {
            "room_id": "NULL",
            "title": "NULL",
            "title_lang": "NULL",
            "room_pic": "NULL",
            "description": "NULL",
            "max_allowed": {
            "total": "NULL",
            "adults": "NULL",
            "children": "NULL",
            "infant": "n/a"
            },
            "no_of_room": "n/a",
            "room_size": "NULL",
            "bed_type": [
                    {
                    "description": "NULL",
                    "configuration": [
                        {
                        "quantity": "NULL",
                        "size": "NULL",
                        "type": "NULL"
                        }
                    ],
                    "max_extrabeds": "n/a"
                    }
                ],
            "shared_bathroom": "n/a"
            },
        "spoken_languages": {
            "type": "NULL",
            "title": "NULL",
            "icon": "NULL"
            },
        "amenities": hotel_room_amenities,
        "facilities": hotel_amenities,
        "hotel_photo": hotel_photo_data, 
        
        "point_of_interests": {
            "code": "NULL",
            "name": "NULL"
            },
        "nearest_airports": {
            "code": "NULL",
            "name": "NULL"
            },
        
        "train_stations": {
            "code": "NULL",
            "name": "NULL"
            },
        
        "connected_locations": [],
        "stadiums": []
    }


    return specific_data



def save_json_files_follow_systemId(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    table = 'hotel_info_all'
    column = 'SystemId'

    systemid_list = get_system_id_list(table, column, engine)

    for systemid in systemid_list:
        file_name = f"{systemid}.json"
        file_path = os.path.join(folder_path, file_name)

        data_dict = get_specifiq_data_from_system_id(table, systemid, engine)

        with open(file_path, "w") as json_file:
            json.dump(data_dict, json_file, indent=4)
            
        print(f"Save {file_name} in {folder_path}")



folder_path = './gill_hotel_json_files'

save_json_files_follow_systemId(folder_path)
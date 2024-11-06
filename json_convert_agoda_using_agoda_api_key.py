import requests
import xmltodict
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)




gtrs_api_key = os.getenv("GTS_API_KEY")


def get_xml_to_json_data_for_agoda(api_key, hotel_id):
    url = f"https://affiliatefeed.agoda.com/datafeeds/feed/getfeed?apikey={api_key}&mhotel_id={hotel_id}&feed_id=19"
    response = requests.get(url)

    if response.status_code == 200:
        xml_data = response.content
        data_dict = xmltodict.parse(xml_data)

        # Ensure "Hotel_feed_full" exists in the parsed data
        hotel_feed_full = data_dict.get("Hotel_feed_full")
        if hotel_feed_full is None:
            print(f"Skipping hotel {hotel_id} as 'Hotel_feed_full' is not found.")
            return None

        hotel_data = hotel_feed_full.get("hotels", {}).get("hotel", {})
        
        if not hotel_data.get("hotel_id"):
            print(f"Skipping hotel {hotel_id} as 'hotel_id' is not found.")
            return None
        
        specific_data = {
            "hotel_id": hotel_data["hotel_id"],
            "name": hotel_data.get("hotel_name", "NULL"),
            "name_local": hotel_data.get("translated_name", "NULL"),
            "hotel_formerly_name": hotel_data.get("hotel_formerly_name", "NULL"),
            "brand_text": "NULL",
            "property_type": hotel_data.get("accommodation_type", "NULL"),
            "star_rating": hotel_data.get("star_rating", "NULL"),
            "chain": "NULL",
            "brand": "NULL",
            "logo": "NULL",
            "primary_photo": "NULL",
            "review_rating": {
                "source": "NULL",
                "number_of_reviews": hotel_data.get("number_of_reviews", "NULL"),
                "rating_average": hotel_data.get("rating_average", "NULL"),
                "popularity_score": hotel_data.get("popularity_score", "NULL"),
            },
            "policies": {
                "check_in": {
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
                "pets": [
                    "Pets not allowed"
                ],
                "remark": "NULL",
                "child_and_extra_bed_policy": {
                    "infant_age": hotel_data.get("child_and_extra_bed_policy", {}).get("infant_age", "NULL"),
                    "children_age_from": hotel_data.get("child_and_extra_bed_policy", {}).get("children_age_from", "NULL"),
                    "children_age_to": hotel_data.get("child_and_extra_bed_policy", {}).get("children_age_to", "NULL"),
                    "children_stay_free": hotel_data.get("child_and_extra_bed_policy", {}).get("children_stay_free", "NULL"),
                    "min_guest_age": hotel_data.get("child_and_extra_bed_policy", {}).get("min_guest_age", "NULL")
                },
                "nationality_restrictions": hotel_data.get("nationality_restrictions", "NULL"),
            },
            "address": {
                "latitude": hotel_data.get("latitude", "NULL"),
                "longitude": hotel_data.get("longitude", "NULL"),
                "address_line_1": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_1", "NULL"),
                "address_line_2": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_2", "NULL"),
                "city": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("city", "NULL"),
                "state": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("state", "NULL"),
                "country": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("country", "NULL"),
                "country_code": "NULL",
                "postal_code": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("postal_code", "NULL"),
                "full_address": "NULL",
                "google_map_site_link": "NULL",
                "local_lang": {
                    "latitude": hotel_data.get("latitude", "NULL"),
                    "longitude": hotel_data.get("longitude", "NULL"),
                    "address_line_1": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_1", "NULL"),
                    "address_line_2": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_2", "NULL"),
                    "city": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("city", "NULL"),
                    "state": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("state", "NULL"),
                    "country": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("country", "NULL"),
                    "country_code": "NULL",
                    "postal_code": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("postal_code", "NULL"),
                    "full_address": "NULL",
                    "google_map_site_link": "NULL",
                },
                "mapping": {
                    "continent_id": "NULL",
                    "country_id": "NULL",
                    "province_id": "NULL",
                    "state_id": "NULL",
                    "city_id": "NULL",
                    "area_id": "NULL"
                }
            },
            "contacts": {
                "phone_numbers": [],
                "fax": "NULL",
                "email_address": "NULL",
                "website": "NULL"
            },
            "descriptions": [
                {
                    "title": "NULL",
                    "text": "NULL"
                }
            ],
            "room_type": [],
            "spoken_languages": [],
            "amenities": [],
            "facilities": [],
            "hotel_photo": [],
            "point_of_interests": [],
            "nearest_airports": [],
            "train_stations": [],
            "connected_locations": [],
            "stadiums": []    
        }

        # Room types processing
        if hotel_feed_full.get("roomtypes") is not None:
            room_types = hotel_feed_full.get("roomtypes", {}).get("roomtype", [])
            for room in room_types:
                if isinstance(room, dict):
                    room_data = {
                        "room_id": room.get("hotel_room_type_id", "NULL"),
                        "title": room.get("standard_caption", "NULL"),
                        "title_lang": room.get("standard_caption", "NULL"),
                        "room_pic": room.get("hotel_room_type_picture", "NULL"),
                        "description": "NULL",
                        "max_allowed": {
                            "total": int(room.get("max_occupancy_per_room", 0)),
                            "adults": int(room.get("max_occupancy_per_room", 0)),
                            "children": "NULL",
                            "infant": room.get("max_infant_in_room", "NULL"),
                        },
                        "no_of_room": room.get("no_of_room", "NULL"),
                        "room_size": room.get("size_of_room", 0),
                        "bed_type": [
                            {
                                "description": room.get("bed_type", "NULL"),
                                "configuration": [],
                                "max_extrabeds": room.get("max_extrabeds", "NULL"),
                            }
                        ],
                        "shared_bathroom": room.get("shared_bathroom", "NULL"),
                    }
                    specific_data["room_type"].append(room_data)
                else:
                    print(f"Skipping room entry as it is not a dictionary: {room}")
        else:
            print(f"Skipping hotel {hotel_id} as 'room_types' is not found")

        # Facilities processing
        facilities_types = hotel_feed_full.get("facilities")
        if facilities_types is None:
            print(f"Skipping hotel {hotel_id} as 'facilities' is ------------------------------------ not found.")
        else:
            facilities_types = facilities_types.get("facility", [])
            if isinstance(facilities_types, list):
                for facility in facilities_types:
                    if isinstance(facility, dict):
                        facilities_data = {
                            "type": facility.get("property_name", "NULL"),
                            "title": facility.get("property_group_description", "NULL"),
                            "icon": facility.get("property_translated_name", "NULL")
                        }
                        specific_data["facilities"].append(facilities_data)
                    else:
                        print(f"Skipping facility entry as it is not a dictionary: {facility}")
            else:
                print(f"No facilities found for hotel {hotel_id}")

        # Hotel photo processing
        if hotel_feed_full.get("pictures") is not None:
            hotel_photo_data = hotel_feed_full["pictures"].get("picture", [])
            for photo in hotel_photo_data:
                if isinstance(photo, dict):
                    hotel_photo_data = {  
                        "picture_id": photo.get("picture_id", "NULL"),
                        "title": photo.get("caption", "NULL"),
                        "url": photo.get("URL", "NULL")
                    }
                    specific_data["hotel_photo"].append(hotel_photo_data)
                else:
                    print(f"Skipping photo entry as it is not a dictionary: {photo}")
        else:
            print(f"Skipping hotel {hotel_id} as 'pictures' is not found.")

        return specific_data
    else:
        print(f"Error fetching data from API for hotel {hotel_id}: Status code {response.status_code}")
        return None


def save_json_to_folder(data, hotel_id, folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    file_path = os.path.join(folder_name, f"{hotel_id}.json")
    try:
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved to {file_path}")
    except TypeError as e:
        print(f"Serialization error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

        
# data = get_xml_to_json_data_for_agoda(api_key=gtrs_api_key, hotel_id=15281267)
# save_json_to_folder(data, hotel_id=15281267, folder_name="Agoda")



def get_vervotech_id(engine, table, providerFamily):
    query = f"SELECT VervotechId FROM {table} WHERE ProviderFamily = '{providerFamily}';"
    df = pd.read_sql(query, engine)
    data = df['VervotechId'].tolist()
    return data



table = "vervotech_mapping"
providerFamily = "Agoda"
ids = get_vervotech_id(engine=engine, table=table, providerFamily=providerFamily)

for id in ids:
    try:
        hotel_id = int(id)  
        data = get_xml_to_json_data_for_agoda(api_key=gtrs_api_key, hotel_id=hotel_id)

        if data is None:
            continue  

        save_json_to_folder(data=data, hotel_id=hotel_id, folder_name=providerFamily)
        print(f"Completed creating JSON file for hotel {hotel_id}")

    except ValueError:
        print(f"Skipping invalid id: {id} (cannot convert to int)")








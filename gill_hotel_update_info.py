import requests
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import pandas as pd


load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)


gill_table = 'gill_hotel_info_table'

def insert_city_data_into_gill_table():
    try:
        query = """
            SELECT CityName,
            MIN(CountryName) AS CountryName,
            MIN(CountryCode) AS CountryCode,
            MIN(PostalCode) AS PostalCode
            FROM vervotech_hotel_list
            GROUP BY CityName;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            data = result.fetchall()
        
        columns = ['CityName', 'CountryName', 'CountryCode', 'PostalCode']
        df = pd.DataFrame(data, columns=columns)

        # for _, row in df.iterrows():
        #     row.to_frame().T.to_sql(name='gill_hotel_info_table', con=engine, if_exists='append', index=False)
        #     print(f"Successfully inserted row: {row.to_dict()}")

        df.to_sql(name='gill_hotel_info_table', con=engine, if_exists='append', index=False)
        print("Data successfully inserted into gill_hotel_info_table.")

    except Exception as e:
        print(f"An error occurred: {e}")

insert_city_data_into_gill_table()

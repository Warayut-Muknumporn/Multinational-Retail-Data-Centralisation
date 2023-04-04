import yaml
import pandas as pd
import sqlalchemy
import tabula
import requests
import json

class DataExtractor:
    
    def __init__(self, creds_file):
        self.connector = DatabaseConnector(creds_file)
    
    def read_data(self, table_name):
        engine = self.connector.init_db_engine()
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            result = conn.execute(query)
            rows = result.fetchall()
        return rows
    
    def read_rds_table(self, table_name):
        engine = self.connector.init_db_engine()
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
        return df
    
    def retrieve_pdf_data(self, link):
        # Read all pages of the PDF document into a list of DataFrames
        pdf_data = tabula.read_pdf(link, pages='all', pandas_options={'header': None})

        # Concatenate all DataFrames into a single DataFrame
        df = pd.concat(pdf_data)

        # Return the cleaned DataFrame
        return df

    def API_key(self):
        return  {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}


    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        response = requests.get(num_stores_endpoint, headers=header_dict)
        if response.status_code == 200:
            return response.json().get('num_stores')
        else:
            print(f"Error: Unable to retrieve number of stores. Response code: {response.status_code}")


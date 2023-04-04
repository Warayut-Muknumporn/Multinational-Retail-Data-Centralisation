import pandas as pd
from datetime import datetime
import numpy as np

class DataCleaning:
    
    def __init__(self, data):
        self.data = data
    
    def clean_user_data(self):
        # Drop rows with null values
        self.data = self.data.dropna()
        
        # Convert date columns to datetime format
        date_columns = ['date_created', 'date_modified']
        for column in date_columns:
            self.data[column] = pd.to_datetime(self.data[column], errors='coerce')
        
        # Convert age column to integer format
        self.data['age'] = pd.to_numeric(self.data['age'], errors='coerce').astype('Int64')
        
        # Remove rows with invalid age values
        self.data = self.data[(self.data['age'] >= 0) & (self.data['age'] <= 120)]
        
        # Remove rows with invalid email values
        self.data = self.data[self.data['email'].str.contains('@')]
        
        # Remove rows with invalid gender values
        self.data = self.data[(self.data['gender'] == 'Male') | (self.data['gender'] == 'Female')]
        
        # Remove rows with invalid occupation values
        valid_occupations = ['Student', 'Teacher', 'Engineer', 'Doctor', 'Lawyer', 'Accountant']
        self.data = self.data[self.data['occupation'].isin(valid_occupations)]
        
        # Remove rows with invalid country values
        valid_countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France']
        self.data = self.data[self.data['country'].isin(valid_countries)]
        
        return self.data

def clean_card_data(self, df):
        # Drop rows with all NULL values
        df = df.dropna(how='all')
        
        # Replace NaN values with empty string
        df = df.fillna('')
        
        # Clean up card number column
        df['Card Number'] = df['Card Number'].str.replace(' ', '')
        df['Card Number'] = df['Card Number'].str.replace('-', '')
        df['Card Number'] = df['Card Number'].str.replace('.', '')
        
        # Clean up expiry date column
        df['Expiry Date'] = df['Expiry Date'].str.replace(' ', '')
        df['Expiry Date'] = df['Expiry Date'].str.replace('/', '')
        df['Expiry Date'] = df['Expiry Date'].str.replace('-', '')
        
        # Convert date column to datetime format
        df['Expiry Date'] = pd.to_datetime(df['Expiry Date'], format='%m%Y', errors='coerce')
        
        # Drop rows with invalid expiry date
        df = df.dropna(subset=['Expiry Date'])
        
        # Convert card type column to categorical format
        df['Card Type'] = df['Card Type'].astype('category')
        
        return df
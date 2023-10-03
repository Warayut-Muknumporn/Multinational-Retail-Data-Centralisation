# Multinational-Retail-Data-Centralisation

This work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

We construct a local PostgreSQL database for this project. We process the data after it is uploaded from various sources, build a database structure, and execute SQL queries. 

In this document, the discussed project is the Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. By utilising the power of Pandas, the project will clean the data, and produce a STAR based database schema for optimised data storage and access. The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. This project will provide the user with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 

# Detail description

-  Developed a system that extracts retail sales data from five different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
-  Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
-  Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
-  Used complex SQL queries to derive insights and to help reduce costs by 15%
-  Queried the data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales. 


## Project 

1. Data extraction. In "data_extraction.py" we store methods responsible for the upload of data into pandas data frame from different sources. 
2. Data cleaning. In "data_cleaning.py" we develop the class DataCleaning that clean different tables, which we uploaded in "data_extraction.py". 
3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the database engine based on credentials provided in ".yml" file.
4. "main.py" contains methods, which allow uploading data directly into the local database. 


## Step by Step Data Processing


1. Remote Postgres database in AWS Cloud. The table "order_table" is the data of the most interest for the client as it contains actual sales information. In the table, we need to use the following fields "date_uuid", "user_uuid", "card_number", "store_code", "product_code" and "product_quantity". The first 5 fields will become foreign keys in our database, therefore we need to clean these columns from all Nans and missing values. The "product_quantity" field has to be an integer.
2. Remote Postgres database in AWS Cloud. The user's data  "dim_users" table. This table is also stored in the remote database, so we use the same upload technics as in the previous case. The primary key here is the "user_uuid" field.
3. Public link in AWS cloud. The "dim_card_details" is accessible by a link from the s3 server and stored as a ".pdf" file. We handle reading ".pdf" using the "tabula" package. The primary key is the card number. The card number has to be converted into a string to avoid possible problems and cleaned from "?" artefacts.
4. The AWS-s3 bucket. The "dim_product" table. We utilise the boto3 package to download this data. The primary key is the "product code" field. The field "product_price" has to be converted into float number and the field "weight" has to convert into grams concerning cases like ("kg", "oz", "l", "ml").
5. The restful-API.  The "dim_store_details" data is available by the GET method. The ".json" response has to be converted into the pandas dataframe. The primary key field is "store_code".
6. The "dim_date_times" data is available by link. The ".json" response has to be converted into the pandas datagrame. The primary key is "date_uuid".

## Data clean
      class DataCleaning:

    def clean_user_data(self,df):
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,'join_date')        
        df = self.clean_NaNs_Nulls_misses(df)
        df.drop(columns='1',inplace=True)
        return df

    def clean_order_data(self,df):
        df.drop(columns='1',inplace=True)
        df.drop(columns='first_name',inplace=True)
        df.drop(columns='last_name',inplace=True)
        df.drop(columns='level_0',inplace=True)
        df['card_number'] = df['card_number'].apply(self.isDigits)
        df.dropna(how='any',inplace= True)
        return df

    def called_clean_store_data(self,df):
        df.drop(columns='lat',inplace=True)
        df                  =  self.clean_invalid_date(df,'opening_date')                     
        df['staff_numbers'] =  pd.to_numeric( df['staff_numbers'].apply(self.remove_char_from_string),errors='coerce', downcast="integer") 
        df.dropna(subset = ['staff_numbers'],how='any',inplace= True)
        return df

    def remove_char_from_string(self,value):
        return re.sub(r'\D', '',value)

    def clean_card_data(self,df):
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace('?','')
        df = self.clean_invalid_date(df,'date_payment_confirmed')  
        df.dropna(how='any',inplace= True)
        return df

    def clean_date_time(self,df):
        df['month']         =  pd.to_numeric( df['month'],errors='coerce', downcast="integer")
        df['year']          =  pd.to_numeric( df['year'], errors='coerce', downcast="integer")
        df['day']           =  pd.to_numeric( df['day'], errors='coerce', downcast="integer")
        df['timestamp']     =  pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df

    def clean_products_data(self,df):
        df =  self.clean_invalid_date(df,'date_added')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df

    def convert_product_weights(self,df,column_name):
        df[column_name] = df[column_name].apply(self.get_grams)
        return df

    def get_grams(self,value):
        value = str(value)
        value = value.replace(' .','')
        if value.endswith('kg'):
            value = value.replace('kg','')
            value = self.check_math_operation(value)
            return 1000*float(value) if self.isfloat(value) else np.nan
        elif value.endswith('g'):   
            value = value.replace('g','')
            value = self.check_math_operation(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('ml'):   
            value = value.replace('ml','')
            value = self.check_math_operation(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('l'):   
            value = value.replace('l','')
            value = self.check_math_operation(value)
            return 1000*float(value) if self.isfloat(value) else np.nan
        elif value.endswith('oz'):   
            value = value.replace('oz','')
            value = self.check_math_operation(value)
            return 28.3495*float(value) if self.isfloat(value) else np.nan
        else:
            np.nan

    def check_math_operation(self,value):
        if 'x' in value:
            value.replace(' ','')
            lis_factors = value.split('x')
            return str(float(lis_factors[0])*float(lis_factors[1]))
        return value

    def isDigits(self,num):
        return str(num) if str(num).isdigit() else np.nan

    def isfloat(self,num):
        try:
            float(num)
            return True
        except ValueError:
            return False
            
    def clean_invalid_date(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        return df

      if __name__ == '__main__':  

    dc = DataCleaning()

    print(str(dc.get_grams('1kg')))
    print(str(dc.get_grams('1g')))
    print(str(dc.get_grams('1l')))
    print(str(dc.get_grams('1ml')))
    print('l1'.isdigit())
    print(str(dc.get_grams('l1ml')))


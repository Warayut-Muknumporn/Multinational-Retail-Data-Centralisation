# Multinational-Retail-Data-Centralisation

This work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

We construct a local PostgreSQL database for this project. We process the data after it is uploaded from various sources, build a database structure, and execute SQL queries. 

In this document, the discussed project is the Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. By utilising the power of Pandas, the project will clean the data, and produce a STAR based database schema for optimised data storage and access. The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. This project will provide the user with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 


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

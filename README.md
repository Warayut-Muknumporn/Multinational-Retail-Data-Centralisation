# Multinational-Retail-Data-Centralisation

This work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

We construct a local PostgreSQL database for this project. We process the data after it is uploaded from various sources, build a database structure, and execute SQL queries. 

In this document, the discussed project is the Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. By utilising the power of Pandas, the project will clean the data, and produce a STAR based database schema for optimised data storage and access. The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. This project will provide the user with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 

# Detail description
- In this project, we create a local PostgreSQL database. We upload data from various sources, process it, create a database schema and run SQL queries.
-  Developed a system that extracts retail sales data from five different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
-  Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
-  Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
-  Used complex SQL queries to derive insights and to help reduce costs by 15%
-  Queried the data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales. 

Key technologies used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas).

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

#### General Data Cleaning Notes

1. All data cleaning must be performed concerning the "primary key" field. Therefore, we remove rows of the table only in the case, if duplicates (NaNs, missing value etc) appear in this field. Otherwise, there is a risk that the "foreign key" in the "orders_table" will not be found in the "primary key" and the database schema would not work.
2. The date transformation has to account for different time formats, so we fix this issue in the following way
```
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
```
Once the clean data is loaded into the database, the data needs to be converted to the appropriate format and a few additional columns added with more information about the data.

Let's consider a typical workflow
1. Convert data fields
```
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE float USING product_price::double precision, 
	ALTER COLUMN weight TYPE float USING weight::double precision, 
	ALTER COLUMN product_code TYPE VARCHAR(255),
	ALTER COLUMN uuid TYPE uuid using uuid::uuid,
	ALTER COLUMN still_available Type Bool using still_available::boolean,
	ALTER COLUMN weight_class Type varchar(50),
	ALTER COLUMN "EAN" Type varchar(255),
```

2. Add foreign and primary keys in connected tables

```
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
ALTER TABLE orders_table 
	ADD FOREIGN KEY(product_code) 
	REFERENCES dim_products(product_code);
```
3. Create additional columns with conditional data segmentation. Here we want to have segments, which will help build store logistics based on product weight. Also, we want to remove string-based availability flags to proper boolean format
```
ALTER TABLE dim_products
	ADD weight_class VARCHAR(30);
UPDATE dim_products
	SET weight_class = 
		CASE 
			when weight/1000 < 2 then 'Light'
			when weight/1000 between 2 and 40 then 'Mid_Sized'
			when weight/1000 between 41 and 140 then 'Heavy'
			when weight/1000 > 140 then 'Truck_Required'  
		else 'Invalid' 
		END;
  
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
  
UPDATE dim_products
	SET still_available = 
		CASE 
			when still_available = 'Still_available' then True
			when still_available = 'Removed' then False
		END;
```

## SQL Queries

As our primary and foreign keys are settled and data are clean, we can start writing queries in our database. 

1. How many stores do the business have and in which countries?
```	
select country_code, 
	count (*) 
from dim_store_details 
group by country_code	
```
<img src="https://user-images.githubusercontent.com/33790455/223984506-becc453f-ebff-4a07-9459-1fa842137abe.png"  width="300">

The query result shows that we have one exception one. After checking it becomes clear, that it is a web store operating internationally. 

2. Which locations have the most stores?	
```
select locality, 
	count (*) 
from dim_store_details group by locality	
ORDER BY COUNT(*) DESC;
```

<img src="https://user-images.githubusercontent.com/33790455/223987621-292b53c5-15e6-4844-8aae-8dd1389d85cd.png"  width="300">

3. Which months produce the most sales overall time of records?

```
select 	dim_date_times.month, 
round(sum(orders_table.product_quantity*dim_products.product_price)) as total_revenue
from orders_table
	join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
	join dim_products on  orders_table.product_code = dim_products.product_code
group by dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC;
```

<img src="https://user-images.githubusercontent.com/33790455/223988320-ea32b8df-834b-45cb-89c2-3041ec835bb5.png" width="300">

4. How many sales come online?

```
select 	count (orders_table.product_quantity) as numbers_of_sales,
	sum(orders_table.product_quantity) as product_quantity_count,
	case 
		when dim_store_details.store_code = 'WEB-1388012W' then 'Web'
	else 'Offline'
	end as product_location
from orders_table
	join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
	join dim_products on  orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by product_location
ORDER BY sum(orders_table.product_quantity) ASC;
```

<img src="https://user-images.githubusercontent.com/33790455/223990808-d5cc1707-3476-4d1e-848e-e080d659d46a.png"  width="370">

5. What percentage of sales come through each type of store?

```
select 	dim_store_details.store_type, 
		round(sum (orders_table.product_quantity*dim_products.product_price)) as revenue,
		round(sum(100.0*orders_table.product_quantity*dim_products.product_price)/(sum(sum(orders_table.product_quantity*dim_products.product_price)) over ())) AS percentage_total
from orders_table
	join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
	join dim_products on  orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by dim_store_details.store_type
ORDER BY percentage_total DESC;
```

<img src="https://user-images.githubusercontent.com/33790455/223994056-0d4f0d86-6737-4617-beba-559482d7b412.png" width="370">

6. Which month in the year produced the most sales?

```
select  dim_date_times.year,
		dim_date_times.month, 
		round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue
from orders_table
	join dim_date_times    on  orders_table.date_uuid    = dim_date_times.date_uuid
	join dim_products      on  orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code    = dim_store_details.store_code
group by 	dim_date_times.month,
			dim_date_times.year
ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;
```

<img src="https://user-images.githubusercontent.com/33790455/223994654-c1105925-130c-45a3-89e2-d03ada8ed18a.png"  width="370">

7. What is the staff count?
```
select  sum(dim_store_details.staff_numbers) as total_staff_numbers, 
	dim_store_details.country_code
from dim_store_details
group by dim_store_details.country_code
```

<img src="https://user-images.githubusercontent.com/33790455/223995198-0203d732-772f-4165-aeea-7e35e5189066.png"  width="300">

8. Which German store saling the most?
```
select  round(count(orders_table.date_uuid)) as sales, 
		dim_store_details.store_type, 
		dim_store_details.country_code
from orders_table
	join dim_date_times    on orders_table.date_uuid    = dim_date_times.date_uuid
	join dim_products      on orders_table.product_code = dim_products.product_code
	join dim_store_details on orders_table.store_code   = dim_store_details.store_code
where dim_store_details.country_code = 'DE'
group by 	dim_store_details.store_type,dim_store_details.country_code
```

<img src="https://user-images.githubusercontent.com/33790455/223996146-fb5c25f0-5a5c-4347-af9b-19a5aac80926.png" width="370">

9. How quickly company making sales?

To perform such a query we need to form a new column in the table dim_times as aggregation statement like avg( LAG() ) is prohibited. 
Therefore we create a new column containing the time difference between timestamps in the dim_date_times table.
```
ALTER TABLE dim_date_times
ADD COLUMN time_diff interval;

UPDATE dim_date_times
SET time_diff = x.time_diff
FROM (
  SELECT timestamp, timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS time_diff
  FROM dim_date_times
) AS x
WHERE dim_date_times.timestamp = x.timestamp;
```
After creation of column time difference, task query much more straightforward
```
select  dim_date_times.year, 		  
    concat('"hours": ',EXTRACT(hours FROM  avg(dim_date_times.time_diff)),' ',
		   '"minutes": ',EXTRACT(minutes FROM  avg(dim_date_times.time_diff)),' ',		  
		   '"seconds": ',round(EXTRACT(seconds FROM  avg(dim_date_times.time_diff)),2),' '		  
		  ) as actual_time_taken		 		  
from dim_date_times
group by dim_date_times.year
order by avg(dim_date_times.time_diff) desc
```
<img src="https://user-images.githubusercontent.com/33790455/223996686-aee796e5-3446-45ac-9114-65f688f0b4fb.png"  width="370">


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
    
## DataExtractor

class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self,engine,table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, con=conn)

    def retrieve_pdf_data(self,link):
        return pd.concat(tabula.read_pdf(link, pages='all'))

    def API_key(self):
        return  {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def retrieve_stores_data(self):
        list_of_frames = []
        store_number   = self.list_number_of_stores()
        for _ in range(store_number):
            api_url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            response = requests.get(
                                    api_url_base,
                                    headers=self.API_key()
                                    )
            list_of_frames.append( pd.json_normalize(response.json()))
        return pd.concat(list_of_frames)
                                
    def list_number_of_stores(self):
        api_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(
                                api_url_base,
                                headers=self.API_key()
                                )
        return response.json()['number_stores']

    def extract_from_s3(self):
        s3_client = boto3.client(
                                    "s3"
                                )
        response = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
        status   = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

    def extract_from_s3_by_link(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        response = requests.get(url) 
        dic      = response.json()
        df       = pd.DataFrame([])
        for column_name in dic.keys():
            value_list = []
            for _ in dic[column_name].keys():
                value_list.append(dic[column_name][_])
            df[column_name] = value_list
        return df

## DatabaseConnector
class DatabaseConnector:

    def __init__(self):
        pass

    def read_db_creds(self,name):
        with open(name, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def init_db_engine(self,cred):
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")
        return engine

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
        
    def upload_to_db(self,df,name,engine):
        df.to_sql(name, engine, if_exists='replace')

      if __name__ == '__main__':
    db = DatabaseConnector()
    engine = db.init_db_engine()
    engine.connect()
    print("Hi") 
    print(engine)
    tables_list = db.list_db_tables(engine)
    print(tables_list)
    with engine.begin() as conn:
        table = pd.read_sql_table(tables_list[1], con=conn)
    print(table)

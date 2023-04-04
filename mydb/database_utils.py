import yaml
import sqlalchemy



class DatabaseConnector:
    
    def __init__(self, creds_file):
        self.creds_file = creds_file

    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)

        return creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        conn_str = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(conn_str)
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        metadata = MetaData(bind=engine)
        metadata.reflect()
        tables = metada
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        return




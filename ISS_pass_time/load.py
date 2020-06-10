
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, TIMESTAMP, String
import pandas as pd
from connection_detail import CONNECTION_DETAIL

class DatabaseConnector:

    def __init__(self):
        self.engine = create_engine(CONNECTION_DETAIL)
        self.connection = self.engine.connect()

    def upload_dataframe(self, df):
        """Upload DataFrame to database."""
        df.to_sql("orbital_data_abigail_beck",
                            self.engine,
                            if_exists='replace',
                            index=False,
                            dtype={"City": Text,
                                    "UTC time": TIMESTAMP,
                                    "duration" : Integer,
                                    "timestamp" : Text
                                    })
        print("Uploaded", len(df), "rows to orbital_data_abigail_beck table.")
        
    def call_stored_procedure(self):
        """Call stored procedure"""
        query = 'call interview.city_stats_abigail_beck();'
        df = pd.read_sql(query, self.engine, index_col=None)
        return df
    
    def create_city_stat(self, cityarg):
        """Create city stats"""
        query = f"""SELECT city, AVG(days) as avg_flight FROM(
		            SELECT city, DATE(UTCtime) AS date, COUNT(city) as days
		            FROM orbital_data_abigail_beck
                    WHERE city = \"{cityarg}\"
                    GROUP BY city, date) as temp
                    GROUP BY city;"""
        df = pd.read_sql(query, self.engine, index_col=None)
        return df
    
    def combine_city_stat(self, ls, keys):
        """Combine city stats"""
        df = pd.concat(ls, keys=keys)
        print("Saving below table into city_stats_combined.csv.")
        print(df)
        df.to_csv("city_stats_combined.csv")

    
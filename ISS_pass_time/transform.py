import pandas as pd
from config import NUM_OF_OUTPUTS, CSV_FILE_PATH
from datetime import datetime

class DataTransformer:
    """Build DataFrame from response."""

    def __init__(self, response:list):
        self.response = response  

    def create_dataframe(self):
        """Make DataFrame out of data received from Fetcher"""
        df = pd.json_normalize(self.response)
        return df
    
    def convert_timestamp(self, df):
        """Convert timestamp to UTC time"""
        df["UTCtime"] = [datetime.fromtimestamp(i) for i in df["timestamp"]]
        df = df.reindex(columns=["city", "UTCtime", "duration", "timestamp"])
        return df


    
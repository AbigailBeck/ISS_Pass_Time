import json
from extract import Fetcher
from transform import DataTransformer
from load import DatabaseConnector
import pandas as pd



def main():
    """Application Entry Point."""
    result = []
    file_path = "city_config_data.txt"
    lines = open(file_path).readlines()
    for line in lines:
        city_config_data = json.loads(line)
        city_data = fetch_pass_times(city_config_data)
        result += city_data

    df = transform_pass_times(result)
    upload_pass_times(df)      

def fetch_pass_times(city_config_data:dict):
    """Fetch ISS Pass Times"""
    print('Fetching ISS Pass Times...')
    fetcher = Fetcher(city_config_data)
    output = fetcher.fetch_results()
    return output

def transform_pass_times(output:dict):
    """Clean data and create pandas DataFrame."""
    print('Transforming output to tabular data...')
    transformer = DataTransformer(output)
    df = transformer.create_dataframe()
    df = transformer.convert_timestamp(df)
    print("Output table:")
    print(df)
    return df

def upload_pass_times(df):
    """Upload table to SQL database."""
    print("Preparing database upload...")
    db = DatabaseConnector()
    db.upload_dataframe(df)
    print("Creating city_stats_abigail_beck...")
    df_ab = db.call_stored_procedure()
    print("Creating city_stats_city...")
    df_haifa = db.create_city_stat("Haifa")
    df_tlv = db.create_city_stat("TelAviv")
    df_bshv = db.create_city_stat("BeerSheva")
    df_eilat = db.create_city_stat("Eilat")
    ls = []
    ls += [df_ab, df_haifa,df_tlv, df_bshv, df_eilat]
    print("Combining tables...")
    db.combine_city_stat(ls, ["city_stats_abigail_beck", 
                                            "city_stats_haifa", 
                                            "city_stats_tel_aviv", 
                                            "city_stats_beer_sheva", 
                                            "city_stats_eilat"])


if __name__ == "__main__":
    main()
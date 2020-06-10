import requests
import json
import pandas as pd
from config import NUM_OF_OUTPUTS 

class Fetcher:

    def __init__(self, city:dict):
        self.endpoint = "http://api.open-notify.org/iss-pass.json"
        self.city = city["City"]
        self.latitude = city["Latitude"]
        self.longtitude = city["Longitude"]
        self.num_of_passes = NUM_OF_OUTPUTS
   
    def fetch_results(self):
        result = {}
        params = {
            "lat": self.latitude,
            "lon": self.longtitude,
            "n": self.num_of_passes}
        req = requests.get(self.endpoint, params=params)
        req =  req.content.decode("utf-8")
        req = json.loads(req)
        
        ls = []

        for i in req["response"]:
            res = {}
            res["city"] = self.city
            res["timestamp"] = i["risetime"]
            res["duration"] = i["duration"]
            ls += [res]
    
        return ls





        

            
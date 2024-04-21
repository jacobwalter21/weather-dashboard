import pandas as pd
from pymongo.mongo_client import MongoClient
import yaml

class mongo_connection:
    def __init__(self, yaml_file):
        self.load_config(yaml_file)
        self.connection = None
    
    def load_config(self, yaml_path="config.yaml"):
        """Load configuration from the YAML file.
        Returns:
            dict: Configuration data.
        """
        with open(yaml_path, "r") as file:
            yaml_file = yaml.safe_load(file)
            
        self.username   = yaml_file["mongo"]["username"]
        self.password   = yaml_file["mongo"]["password"]
        self.cluster    = yaml_file["mongo"]["cluster"]
        self.database   = yaml_file["mongo"]["database"]
        self.collection = yaml_file["mongo"]["collection"]
        
    def establish_connection(self):
        """Create a Mongo DB connection using the configuration.
        Returns:
            Mongo Connection object.
        """
        url = "mongodb+srv://" + self.username + ":" + self.password + "@" + self.cluster
        self.connection = MongoClient(url)
        
    def write_to_mongo(self, df):
        """Write a Dataframe to Mongo Database
        """
        print("Writing to Mongo DB")
        db = self.connection[self.database]
        coll = db[self.collection]
        coll.insert_many(df.to_dict('records'))
from pymongo import MongoClient
import logging
from datetime import datetime
from settings import MONGO_URI, DB_NAME_TEMPLATE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDBPipeline:
    def __init__(self, collection_name):
        current_date = datetime.now()
        if current_date.month == 12:
            next_month = 1
            year = current_date.year + 1
        else:
            next_month = current_date.month + 1
            year = current_date.year

        self.db_name = DB_NAME_TEMPLATE.format(year=year, next_month=next_month)
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[self.db_name]
        self.collection = self.db[collection_name]
        self.collection.create_index("website", unique=True)

    def insert_agent(self, item):
        try:
            self.collection.insert_one(item)
            logging.info("Inserted agent: %s", item.get("first_name"))
        except Exception as e:
            logging.warning("Failed to insert agent: %s", e)

    def update_agent(self, agent_id, update_data):
        self.collection.update_one({"_id": agent_id}, {"$set": update_data})

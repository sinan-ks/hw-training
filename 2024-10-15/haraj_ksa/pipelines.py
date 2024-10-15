import logging
from settings import DB_CLIENT

def save_to_mongo(properties, collection_name):
    if properties:
        collection = DB_CLIENT['propertyfinder_monthly_2024_10'][collection_name]
        for item in properties:
            try:
                collection.insert_one(item)
                logging.info(f"Inserted property with id: {item['id']} into MongoDB.")
            except Exception as e:
                logging.error(f"Error inserting property {item['id']}: {e}")

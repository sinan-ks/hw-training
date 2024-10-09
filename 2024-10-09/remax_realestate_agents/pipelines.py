from pymongo import errors
import logging
from settings import mongo_client, DB_NAME, COLLECTION_PROFILES

class ProfilePipeline:
    def __init__(self):
        self.db = mongo_client[DB_NAME]
        self.profiles_collection = self.db[COLLECTION_PROFILES]

    def save_profile_to_mongodb(self, profile_data, url):
        """Save the extracted profile data to MongoDB."""
        profile_data['profile_url'] = url
        try:
            self.profiles_collection.update_one(
                {'profile_url': url},  
                {'$set': profile_data},
                upsert=True  
            )
            logging.info(f"Inserted/Updated profile for {profile_data['first_name']} {profile_data['last_name']}.")
        except errors.DuplicateKeyError:
            logging.warning(f"Duplicate profile found for {profile_data['first_name']} {profile_data['last_name']}. Skipping.")
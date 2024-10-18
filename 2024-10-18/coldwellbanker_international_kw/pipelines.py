import logging

class MongoDBPipeline:
    def __init__(self, db, collection_name):
        """Initializes the MongoDB collection."""
        self.collection = db[collection_name]
        self.collection.create_index("profile_url", unique=True)

    def insert(self, data):
        """Inserts data into the MongoDB collection."""
        try:
            self.collection.insert_one(data)
            logging.info(f"Inserted profile: {data['profile_url']}")
        except Exception as e:
            logging.error(f"Error inserting profile {data['profile_url']}: {str(e)}")

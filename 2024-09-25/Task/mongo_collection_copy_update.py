import logging
from pymongo import MongoClient
from urllib.parse import urlparse
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDBCollectionCopier:
    def __init__(self, db_name, host='localhost', port=27017):
        self.client = MongoClient(f'mongodb://{host}:{port}/')
        self.db = self.client[db_name]
        logging.info(f'Connected to database: {db_name}')

    def copy_collection(self, original_collection_name, new_collection_name):
        original_collection = self.db[original_collection_name]
        documents = list(original_collection.find())  # Retrieve all documents
        
        if documents:
            self.db[new_collection_name].insert_many(documents)  
            logging.info(f'Copied {len(documents)} documents from {original_collection_name} to {new_collection_name}.')
        else:
            logging.warning(f"No documents found in {original_collection_name} to copy.")

    def update_collection(self, collection_name):
        collection = self.db[collection_name]
        
        for document in collection.find():
            updates = {}
            unique_id = document.get('unique_id')

            if document.get('regular_price') and document.get('selling_price'):
                updates['currency'] = 'Swiss franc'
                
            image_urls = document.get('image_urls', [])
            
            if updates:
                collection.update_one({'_id': document['_id']}, {'$set': updates})

            # Process image URLs and create file names
            for index, image_url in enumerate(image_urls):
                url_path = urlparse(image_url).path
                file_extension = os.path.splitext(url_path)[1]

                file_name_key = f'file_name_{index + 1}'
                image_url_key = f'image_url_{index + 1}'

                file_name = f'{unique_id}_{index + 1}{file_extension}'
                collection.update_one({'_id': document['_id']}, {'$set': {file_name_key: file_name, image_url_key: image_url}})
                
                logging.info(f'Updated document with _id: {document["_id"]} with new fields: {file_name_key}, {image_url_key}.')

            # Remove the image_urls field after processing
            collection.update_one({'_id': document['_id']}, {'$unset': {'image_urls': ""}})
            logging.info(f'Removed image_urls field from document with _id: {document["_id"]}.')

    def close(self):
        self.client.close()
        logging.info('Connection closed.')

if __name__ == "__main__":
    copier = MongoDBCollectionCopier('lidl')
    copier.copy_collection('product_details', 'product_details_v2')
    copier.update_collection('product_details_v2')
    copier.close()

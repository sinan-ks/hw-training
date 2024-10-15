import json
import logging
from settings import DB_CLIENT

class MongoExporter:
    def __init__(self, collection_name, output_file):
        """Initialize the MongoExporter with the collection name and output file."""
        self.collection_name = collection_name
        self.output_file = output_file
        self.collection = DB_CLIENT['propertyfinder_monthly_2024_10'][collection_name]

    def export_to_json(self):
        """Export the MongoDB collection to a JSON Lines file, excluding the _id field."""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as file:  
                for document in self.collection.find():
                    document.pop('_id', None)  
                    json_line = json.dumps(document, ensure_ascii=False)  
                    file.write(json_line + '\n')  
                    logging.info(f"Exported document: {json_line}")

            logging.info(f"Export completed successfully. Data saved to {self.output_file}.")

        except Exception as e:
            logging.error(f"An error occurred during export: {e}")

def main():
    """Main function to execute the export."""
    logging.basicConfig(level=logging.INFO)
    exporter = MongoExporter('haraj_property_details', 'haraj_ksa_2024_10.json') 
    exporter.export_to_json() 

if __name__ == "__main__":
    main()

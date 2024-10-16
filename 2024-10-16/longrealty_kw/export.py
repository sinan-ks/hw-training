import json
import logging
from pipelines import MongoDBPipeline 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDBExporter:
    def __init__(self, collection_name):
        self.pipeline = MongoDBPipeline(collection_name)
        self.year = self.pipeline.db_name.split('_')[2]  
        self.month = self.pipeline.db_name.split('_')[3]  

    def export_to_json(self):
        file_name = f"longrealty_{self.year}_{self.month}_05.json" 
        try:
            with open(file_name, 'w', encoding='utf-8') as json_file:
                cursor = self.pipeline.collection.find({})
                for document in cursor:
                    document.pop('_id', None) 
                    json_file.write(json.dumps(document, ensure_ascii=False) + '\n')
            logging.info("Exported data to %s successfully.", file_name)
        except Exception as e:
            logging.error("Failed to export data: %s", e)

def main():
    collection_name = 'agents' 
    exporter = MongoDBExporter(collection_name)
    exporter.export_to_json()

if __name__ == "__main__":
    main()

import json
from settings import get_database, DATA_COLLECTION_NAME, PROJECT

def export_data_to_json():
    """Fetches data from MongoDB and exports it as a JSON file."""
    db, YYY_MM_DD = get_database()
    file_name = f"{PROJECT}_{YYY_MM_DD}.json"
    
    collection = db[DATA_COLLECTION_NAME]

    with open(file_name, 'w', encoding='utf-8') as json_file:
        cursor = collection.find({}, {'_id': False})  
        for document in cursor:
            json_line = json.dumps(document, ensure_ascii=False) 
            json_file.write(json_line + '\n')

    print(f"Data exported to {file_name} successfully.")

def main():
    export_data_to_json()

if __name__ == "__main__":
    main()

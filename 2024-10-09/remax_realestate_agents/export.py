import json
import logging
from collections import OrderedDict
from settings import mongo_client, DB_NAME, COLLECTION_PROFILES

class MongoExporter:
    def __init__(self):
        self.db = mongo_client[DB_NAME]
        self.profiles_collection = self.db[COLLECTION_PROFILES]

    def export_profiles_to_json(self, output_file):
        """Export profile data from MongoDB to a JSON file, line by line, and in the specified field order."""
        try:
            profiles_cursor = self.profiles_collection.find({}, {'_id': 0})

            with open(output_file, 'w', encoding='utf-8') as file:
                for profile in profiles_cursor:
                    ordered_profile = OrderedDict([
                        ('first_name', profile.get('first_name', '')),
                        ('middle_name', profile.get('middle_name', '')),
                        ('last_name', profile.get('last_name', '')),
                        ('office_name', profile.get('office_name', '')),
                        ('title', profile.get('title', '')),
                        ('description', profile.get('description', '')),
                        ('languages', profile.get('languages', [])),
                        ('image_url', profile.get('image_url', '')),
                        ('address', profile.get('address', '')),
                        ('city', profile.get('city', '')),
                        ('state', profile.get('state', '')),
                        ('country', profile.get('country', 'United States')),
                        ('zipcode', profile.get('zipcode', '')),
                        ('office_phone_numbers', profile.get('office_phone_numbers', [])),
                        ('agent_phone_numbers', profile.get('agent_phone_numbers', [])),
                        ('email', profile.get('email', '')),
                        ('website', profile.get('website', '')),
                        ('social', profile.get('social', {})),
                        ('profile_url', profile.get('profile_url', ''))
                    ])
                    
                    file.write(json.dumps(ordered_profile, ensure_ascii=False) + '\n')

            logging.info(f"Exported profiles to {output_file}.")
        except Exception as e:
            logging.error(f"Failed to export profiles to JSON. Error: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the exporter...")
    exporter = MongoExporter()
    exporter.export_profiles_to_json('remax_realestate_2024_10_09.json')
    logging.info("Exporter finished.")
    
if __name__ == "__main__":
    main()
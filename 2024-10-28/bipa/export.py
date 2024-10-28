import csv
from mongoengine import connect
from settings import MONGO_DB_NAME, MONGO_URI, FILE_NAME
from items import ProductItem

class DataExporter:
    def __init__(self, mongo_db_name, mongo_uri, file_name):
        self.mongo_db_name = mongo_db_name
        self.mongo_uri = mongo_uri
        self.file_name = file_name

    def export_to_csv(self):
        """Connect to MongoDB and export data to a CSV file."""
        connect(self.mongo_db_name, host=self.mongo_uri)
        print(f"Connected to MongoDB: {self.mongo_db_name}")

        # Fetch all product data
        product_data_list = ProductItem.objects()

        if not product_data_list:
            print("No data found to export.")
            return

        # Get the first product's keys to establish field order
        first_product = product_data_list[0].to_mongo()
        all_keys = [key for key in first_product.keys() if key != '_id']  

        with open(self.file_name, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')

            # Write the header using the order from the first document
            writer.writerow(all_keys)

            # Write each product data row
            for product in product_data_list:
                row = [str(product.to_mongo().get(field, '')).replace('|', '\\|') for field in all_keys]
                writer.writerow(row)

        print(f"Data successfully exported to {self.file_name}")

def main():
    exporter = DataExporter(MONGO_DB_NAME, MONGO_URI, FILE_NAME)
    exporter.export_to_csv()

if __name__ == "__main__":
    main()

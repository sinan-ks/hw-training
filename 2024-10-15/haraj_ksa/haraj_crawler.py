import requests
from datetime import datetime
import logging
from settings import INPUTS, HEADERS, BASE_URL, API_URL
from pipelines import save_to_mongo

logging.basicConfig(level=logging.INFO)

class HarajCrawler:
    def __init__(self, collection_name):
        self.base_url = BASE_URL
        self.api_url = API_URL
        self.headers = HEADERS
        self.inputs = INPUTS
        self.collection_name = collection_name

    def fetch_data(self, slug, page=1):
        json_data = {
            'query': '''
            query FetchAds($page: Int, $tag: String) {
                posts(page: $page, tag: $tag) {
                    items {
                        id
                        title
                        updateDate
                        authorUsername
                        URL
                        bodyTEXT
                        city
                        imagesList
                    }
                }
            }''',
            'variables': {
                'tag': slug,
                'page': page,
            },
        }

        response = requests.post(self.api_url, headers=self.headers, json=json_data)
        if response.status_code == 200:
            return response.json()
        logging.error(f"Error fetching data: {response.status_code}")
        return None

    def format_date(self, timestamp):
        if isinstance(timestamp, int):  
            try:
                date = datetime.utcfromtimestamp(timestamp)
                return date.strftime('%Y-%m-%d')  
            except ValueError:
                logging.warning("Error formatting date.")
        return ""

    def extract_properties(self, data, current_slug):
        properties = data.get('data', {}).get('posts', {}).get('items', [])
        current_date = datetime.now().strftime('%Y-%m-%d')
        extracted_properties = []

        input_data = next((item for item in self.inputs if item['slug'] == current_slug), "")

        if input_data:
            category = input_data["category"]
            category_url = input_data["category_url"]
            property_type = input_data["property_type"]
            sub_category_1 = input_data["sub_category_1"]

        for property in properties:
            property_url = property.get('URL', '')
            if not property_url.startswith(self.base_url):
                property_url = self.base_url + property_url

            update_date = property.get('updateDate', '')
            published_date = self.format_date(update_date) if update_date else ""

            description = property.get('bodyTEXT', '')
            cleaned_description = ' '.join(description.split())

            item = {}
            item["id"] = '11' + str(property.get('id', ''))
            item["url"] = property_url
            item["broker_display_name"] = property.get('authorUsername', '')
            item["broker"] = property.get('authorUsername', '')
            item["category"] = category
            item["category_url"] = category_url
            item["title"] = property.get('title', '')
            item["property_type"] = property_type
            item["sub_category_1"] = sub_category_1
            item["sub_category_2"] = ""
            item["description"] = cleaned_description
            item["location"] = property.get('city', '')
            item["depth"] = ""
            item["price"] = ""
            item["currency"] = ""
            item["price_per"] = ""
            item["bedrooms"] = ""
            item["bathrooms"] = ""
            item["furnished"] = ""
            item["rera_permit_number"] = ""
            item["dtcm_licence"] = ""
            item["scraped_ts"] = current_date
            item["amenities"] = ""
            item["details"] = ""
            item["agent_name"] = ""
            item["reference_number"] = ""
            item["number_of_photos"] = str(len(property.get('imagesList', [])))
            item["user_id"] = property.get('authorUsername', '')
            item["published_at"] = published_date
            item["phone_number"] = ""
            item["date"] = current_date
            item["iteration_number"] = '2024_10'
            item["latitude"] = ""
            item["longitude"] = ""

            extracted_properties.append(item)

        return extracted_properties

    def run_crawler(self, target_data_count=100):
        for input_data in self.inputs:
            tag = input_data["slug"]
            logging.info(f"Starting crawl for tag: {tag}")
            total_data_count = 0
            page = 1

            while total_data_count < target_data_count:
                data = self.fetch_data(tag, page=page)
                if not data:
                    break

                extracted_properties = self.extract_properties(data, current_slug=tag)
                save_to_mongo(extracted_properties, self.collection_name)

                total_data_count += len(extracted_properties)
                if len(extracted_properties) == 0:  
                    break

                page += 1
                logging.info(f"Total data collected for {tag}: {total_data_count}")

def main():
    """Main function to execute the crawler."""
    logging.basicConfig(level=logging.INFO)
    crawler = HarajCrawler(collection_name='haraj_property_details')
    crawler.run_crawler(target_data_count=100)

if __name__ == "__main__":  
    main()

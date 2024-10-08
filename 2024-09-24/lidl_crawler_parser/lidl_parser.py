import requests
from parsel import Selector
from pymongo import MongoClient, errors
import re
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LidlParser:
    def __init__(self, db_name, collection_name, json_file):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.product_details_collection = self.db['product_details']
        self.product_details_collection.create_index('pdp_url', unique=True)
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://sortiment.lidl.ch/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        }
        self.json_file = json_file

    def get_product_urls(self):
        """Fetch all product URLs from the MongoDB collection."""
        logging.info("Fetching product URLs from MongoDB.")
        return self.collection.find({}, {'url': 1, '_id': 0})

    def normalize_price(self, price_list):
        """Normalize price strings by removing unwanted characters and spaces."""
        if price_list:
            price = ' '.join([price.strip().replace('*', '') for price in price_list]).strip()
            price = re.sub(r'\s+', ' ', price) 
            price = price.replace('CHF', '').strip()
            price = re.sub(r'(\d+)\s*.\s*(\d+)', r'\1.\2', price) 
            
            # Split by spaces to handle multiple prices and take the first one
            first_price = price.split()[0] if price.split() else ''
            
            try:
                return float(first_price) if first_price else ''
            except ValueError:
                logging.error(f"Could not convert price '{first_price}' to float.")
                return ''
        return ''

    def normalize_price_per_unit(self, price_per_unit_list):
        """Clean and normalize the price per unit string."""
        if price_per_unit_list:
            price_per_unit = ' '.join([ppu.strip() for ppu in price_per_unit_list]).strip()
            price_per_unit = re.sub(r'\s*\|\s*$', '', price_per_unit)  # Remove trailing pipes (|) if present
            return price_per_unit.strip()
        return ''

    def parse_product_details(self, product_url):
        """Parse product details from the given product page URL using parsel."""
        logging.info(f"Fetching product details from {product_url}")
        try:
            response = requests.get(product_url, headers=self.headers)
            response.raise_for_status()
            
            selector = Selector(response.text)

            # Extract the script content containing the image data
            script_content = selector.xpath('//script[contains(text(),"mage/gallery/gallery")]/text()').get()
            
            if script_content:
                # Extract JSON-like structure from the script 
                json_match = re.search(r'\{.*"data":\[(.*?)\].*\}', script_content)
                if json_match:
                    json_data = f'{{"data":[{json_match.group(1)}]}}'
                    image_data = json.loads(json_data)
                    
                    # Extract image URLs from the "data" key
                    image_urls = [image['img'] for image in image_data['data']]
                else:
                    image_urls = []
            else:
                image_urls = []

            unique_id_text = selector.xpath('//p[@class="sku text-gray"]/text()').extract_first()
            unique_id = re.search(r'\d+', unique_id_text).group() if unique_id_text else ''

            product_name = selector.xpath('//div[@class="page-title-wrapper product"]//span[@itemprop="name"]//text()').extract()
            product_name = ' '.join([name.strip() for name in product_name]).strip() if product_name else ''

            brand = selector.xpath('//div[@class="brand-details"]//p[@class="brand-name"]//text()').extract_first()
            brand = brand.strip() if brand else ''

            regular_price = self.normalize_price(selector.xpath('//del[@class="pricefield__old-price"]//text()').extract())
            selling_price = self.normalize_price(selector.xpath('//strong[@class="pricefield__price"]//text()').extract())

            # If regular price is missing, set it to the selling price
            regular_price = regular_price if regular_price else selling_price

            price_per_unit = self.normalize_price_per_unit(selector.xpath('//span[@class="pricefield__footer"]//text()').extract())

            breadcrumb = selector.xpath('//div[@class="breadcrumbs"]//ul[@class="items"]//text()').extract()
            breadcrumb = [crumb.strip() for crumb in breadcrumb if crumb.strip()]

            product_hierarchy = [''] * 7  # Initialize all levels with empty strings
            for i, level in enumerate(breadcrumb):
                if i < 7:  # Only fill up to 7 levels
                    product_hierarchy[i] = level

            product_description = selector.xpath('//div[@id="tab-description"]//div[@class="col-left"]/p//text()').extract()
            product_description = ' '.join([desc.strip() for desc in product_description]).strip() if product_description else ''

            product_details = {
                'unique_id': unique_id,
                'product_name': product_name,
                'brand': brand,
                'regular_price': regular_price,
                'selling_price': selling_price,
                'price_per_unit': price_per_unit,
                'breadcrumb': ' > '.join(breadcrumb),
                'pdp_url': product_url,
                'product_description': product_description,
                'producthierarchy_level1': product_hierarchy[0],
                'producthierarchy_level2': product_hierarchy[1],
                'producthierarchy_level3': product_hierarchy[2],
                'producthierarchy_level4': product_hierarchy[3],
                'producthierarchy_level5': product_hierarchy[4],
                'producthierarchy_level6': product_hierarchy[5],
                'producthierarchy_level7': product_hierarchy[6],
                'image_urls': image_urls  
            }

            return product_details
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for URL {product_url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Error parsing product details for URL {product_url}: {e}")
            return None

    def save_product_details(self, product_details):
        """Save product details to the MongoDB collection and JSON file."""
        try:
            self.product_details_collection.insert_one(product_details)
            logging.info(f"Inserted product details for URL: {product_details['pdp_url']}")
            
            product_details.pop('_id', None)
            
            with open(self.json_file, 'a', encoding='utf-8') as f:
                json.dump(product_details, f, ensure_ascii=False)
                f.write('\n') 
                logging.info(f"Saved product details to JSON file for URL: {product_details['pdp_url']}")
        
        except errors.DuplicateKeyError:
            logging.info(f"Duplicate product details found and skipped for URL: {product_details['pdp_url']}")
        except IOError as e:
            logging.error(f"Failed to write to JSON file: {e}")

    def parse_all_products(self):
        """Parse details for all products stored in the URLs collection."""
        logging.info("Starting to parse all products.")
        product_urls = self.get_product_urls()
        for product in product_urls:
            product_url = product['url']
            product_details = self.parse_product_details(product_url)
            if product_details:
                self.save_product_details(product_details)
        logging.info("Finished parsing all products.")

if __name__ == "__main__":
    db_name = 'lidl'
    collection_name = 'product_urls'
    json_file = 'product_details.json'

    parser = LidlParser(db_name, collection_name, json_file)
    parser.parse_all_products()

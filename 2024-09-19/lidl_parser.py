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

    def parse_product_details(self, product_url):
        """Parse product details from the given product page URL using parsel."""
        logging.info(f"Fetching product details from {product_url}")
        try:
            response = requests.get(product_url, headers=self.headers)
            response.raise_for_status()
            
            selector = Selector(response.text)
            
            product_name = selector.xpath('//div[@class="page-title-wrapper product"]//span[@itemprop="name"]//text()').getall()
            product_name = ' '.join([name.strip() for name in product_name]).strip() if product_name else ''
            
            brand_img_src = selector.xpath('//img[@class="product-brand-logo"]/@src').get()
            brand = brand_img_src.split('/')[-1].replace('.png', '') if brand_img_src else ''
            
            regular_price = selector.xpath('//del[@class="pricefield__old-price"]//text()').getall()
            if regular_price:
                regular_price = ' '.join([price.strip().replace('*', '') for price in regular_price]).strip()
                regular_price = re.sub(r'\s+', ' ', regular_price)  # Remove extra spaces
                regular_price = regular_price.replace('CHF', '').strip()
                regular_price = re.sub(r'(\d+)\s*.\s*(\d+)', r'\1.\2', regular_price)  # Normalize price format
                regular_price = float(regular_price) if regular_price else ''
            else:
                regular_price = ''  
            
            selling_price = selector.xpath('//strong[@class="pricefield__price"]//text()').getall()
            if selling_price:
                selling_price = ' '.join([price.strip().replace('*', '') for price in selling_price]).strip()
                selling_price = re.sub(r'\s+', ' ', selling_price) 
                selling_price = selling_price.replace('CHF', '').strip()
                selling_price = re.sub(r'(\d+)\s*.\s*(\d+)', r'\1.\2', selling_price) 
                selling_price = float(selling_price) if selling_price else ''
            else:
                selling_price = ''  
            
            if not regular_price and selling_price:
                regular_price = selling_price
            
            price_per_unit = selector.xpath('//span[@class="pricefield__footer"]//text()').getall()
            price_per_unit = ' '.join([ppu.strip() for ppu in price_per_unit]).strip() if price_per_unit else ''
            
            breadcrumb = selector.xpath('//div[@class="breadcrumbs"]//ul[@class="items"]//text()').getall()
            breadcrumb = ' > '.join([crumb.strip() for crumb in breadcrumb if crumb.strip()]).strip() if breadcrumb else ''
            
            product_description = selector.xpath('//div[@id="tab-description"]//div[@class="col-left"]/p//text()').getall()
            product_description = ' '.join([desc.strip() for desc in product_description]).strip() if product_description else ''

            # Cleaning and joining the extracted data
            product_details = {
                'product_name': product_name,
                'brand': brand.strip(),
                'regular_price': regular_price,
                'selling_price': selling_price,
                'price_per_unit': price_per_unit,
                'breadcrumb': breadcrumb,
                'pdp_url': product_url,
                'product_description': product_description,
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

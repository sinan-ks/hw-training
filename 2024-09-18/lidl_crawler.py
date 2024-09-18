import requests
from lxml import html
import logging
from pymongo import MongoClient, errors
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LidlCrawler:
    def __init__(self, base_url, db_name, collection_name, max_pages):
        self.base_url = base_url
        self.max_pages = max_pages
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.collection.create_index('url', unique=True)
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://sortiment.lidl.ch/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        }

    def scrape_page(self, page_number):
        url = self.base_url.format(page=page_number)
        logging.info(f"Fetching the page content from {url}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  
            logging.info("Successfully fetched the page content")

            tree = html.fromstring(response.content)
            product_links = tree.xpath('//div[@class="slide"]/a[@class="product-item-link"]/@href')
            
            if product_links:
                logging.info(f"Found {len(product_links)} product links on page {page_number}")
                return product_links
            else:
                logging.warning(f"No product links found on page {page_number}")
                return []
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return []

    def insert_urls(self, urls):
        for url in urls:
            full_url = url if url.startswith('http') else f"https://sortiment.lidl.ch{url}"
            try:
                self.collection.insert_one({'url': full_url})
                logging.info(f"Inserted URL: {full_url}")
            except errors.DuplicateKeyError:
                logging.info(f"Duplicate URL found and skipped: {full_url}")

    def crawl(self):
        logging.info("Starting crawl process...")
        all_product_links = []

        for page_number in range(1, self.max_pages + 1):
            links = self.scrape_page(page_number)
            if not links:
                logging.info("No more products found. Stopping pagination.")
                break
            all_product_links.extend(links)
            time.sleep(1) 

        self.insert_urls(all_product_links)
        logging.info(f"Total product links found and inserted: {len(all_product_links)}")
        self.client.close() 

if __name__ == "__main__":
    base_url = 'https://sortiment.lidl.ch/de/alle-kategorien?p={page}#'
    db_name = 'lidl'
    collection_name = 'product_urls'
    max_pages = 140

    crawler = LidlCrawler(base_url, db_name, collection_name, max_pages)
    crawler.crawl()

import requests
from parsel import Selector
from pymongo import MongoClient, errors
import logging
from urllib.parse import urljoin
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LidlCrawler:
    def __init__(self, start_url, db_name, collection_name):
        self.start_url = start_url
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.collection.create_index('url', unique=True)
        logging.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")

        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://sortiment.lidl.ch/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        }

    def scrape_page(self, url):
        """ Fetch product links from a given page. """
        logging.info(f"Fetching page content from {url}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            logging.info("Successfully fetched the page content")

            selector = Selector(response.text)
            product_links = selector.xpath('//div[@class="slide"]/a[@class="product-item-link"]/@href').extract()
            
            if product_links:
                logging.info(f"Found {len(product_links)} product links on page {url}")
                full_product_links = [urljoin(url, link) for link in product_links] 
                next_page_url = selector.xpath('//link[@rel="next"]/@href').extract_first()
                next_page_url = urljoin(url, next_page_url) if next_page_url else None
                return full_product_links, next_page_url
            else:
                logging.warning(f"No product links found on page {url}")
                return [], None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return [], None

    def insert_urls(self, urls):
        """ Insert the product URLs into the database. """
        for url in urls:
            try:
                self.collection.insert_one({'url': url})
                logging.info(f"Inserted URL into DB: {url}")
            except errors.DuplicateKeyError:
                logging.info(f"Duplicate URL found and skipped: {url}")
            except errors.PyMongoError as e:
                logging.error(f"Failed to insert URL {url} into DB: {e}")

    def crawl(self):
        """ Start the crawling process from the start URL, following pagination links. """
        logging.info("Starting crawl process...")
        all_product_links = []
        current_url = self.start_url

        while current_url:
            product_links, next_page_url = self.scrape_page(current_url)
            if not product_links:
                logging.info("No more product links found. Stopping pagination.")
                break

            self.insert_urls(product_links)
            all_product_links.extend(product_links)
            current_url = next_page_url
            time.sleep(1)  

        logging.info(f"Total product links found and inserted: {len(all_product_links)}")
        self.client.close()  
        logging.info("Crawl process completed.")

if __name__ == "__main__":
    start_url = 'https://sortiment.lidl.ch/de/alle-kategorien?p=1#'
    db_name = 'lidl'
    collection_name = 'product_urls'

    crawler = LidlCrawler(start_url, db_name, collection_name)
    crawler.crawl()

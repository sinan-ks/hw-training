import requests
from parsel import Selector
from urllib.parse import urljoin
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MytheresaCrawler:
    def __init__(self, base_url, db_name, collection_name):
        self.base_url = base_url
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'sec-fetch-user': '?1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        }

    def get_product_links(self, page_url):
        try:
            response = requests.get(page_url, headers=self.headers, timeout=10) 
            response.raise_for_status()  
            selector = Selector(text=response.text)
            
            # Extract product URLs 
            product_links = selector.xpath('//div[contains(@class, "item")]//a[@class="item__link"]/@href').getall()
            for link in product_links:
                full_url = urljoin(page_url, link)
                self.save_url_to_db(full_url)

            # Check for the next page and crawl it
            next_page = selector.xpath('//div[contains(@class, "loadmore__button")]//a/@href').get()
            if next_page:
                full_next_page_url = urljoin(page_url, next_page)
                self.get_product_links(full_next_page_url)

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")

    def save_url_to_db(self, url):
        if not self.collection.find_one({"url": url}):
            self.collection.insert_one({"url": url})
            logging.info(f"Saved URL to DB: {url}")
        else:
            logging.info(f"URL already exists in DB: {url}")

    def crawl(self):
        logging.info("Starting crawl process...")
        self.get_product_links(self.base_url)
        logging.info("Crawl process completed.")
        self.client.close() 

if __name__ == "__main__":
    base_url = 'https://www.mytheresa.com/int_en/men/shoes.html'
    db_name = 'mytheresa'
    collection_name = 'product_urls'

    crawler = MytheresaCrawler(base_url, db_name, collection_name)
    crawler.crawl()

import requests
from parsel import Selector
from urllib.parse import urljoin
from pymongo import MongoClient

class MytheresaCrawler:
    def __init__(self, base_url, db_name, collection_name):
        self.base_url = base_url
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def get_product_links(self, page_url):
        response = requests.get(page_url)
        if response.status_code == 200:
            selector = Selector(text=response.text)
            
            # Extract product URLs
            product_links = selector.css('div.item a::attr(href)').getall()
            for link in product_links:
                full_url = urljoin(page_url, link)
                self.save_url_to_db(full_url)

            # Check for the next page and crawl it
            next_page = selector.xpath('//a[contains(@class, "pagination__item") and contains(@class, "icon__next")]/@href').get()
            if next_page:
                full_next_page_url = urljoin(page_url, next_page)
                self.get_product_links(full_next_page_url)

    def save_url_to_db(self, url):
        # Insert URL into MongoDB collection if it doesn't already exist
        if not self.collection.find_one({"url": url}):
            self.collection.insert_one({"url": url})

    def crawl(self):
        self.get_product_links(self.base_url)

if __name__ == "__main__":
    base_url = 'https://www.mytheresa.com/int_en/men/shoes.html'
    db_name = 'mytheresa'
    collection_name = 'product_urls'

    crawler = MytheresaCrawler(base_url, db_name, collection_name)
    crawler.crawl()

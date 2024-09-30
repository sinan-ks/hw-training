import requests
import re
from scrapy import Selector
from pymongo import MongoClient
import logging
from pymongo.errors import DuplicateKeyError

class AgentScraper:
    def __init__(self, url, db_name, collection_name, mongo_host='localhost', mongo_port=27017):
        self.client = MongoClient(mongo_host, mongo_port)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.collection.create_index("profile_url", unique=True)
        self.url = url
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'Referer': 'https://www.alliebeth.com/roster/Agents',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)       

    def fetch_data(self):
        """Fetch data from the URL"""
        self.logger.info(f"Fetching data from {self.url}")
        response = requests.get(url=self.url, headers=self.headers)
        return Selector(text=response.text)

    def parse_agent(self, selector):
        """Parse individual agent details"""
        profile_url = selector.xpath('.//a[@class="site-roster-card-image-link"]/@href').extract_first()
        if not profile_url:
            return None

        image_url = selector.xpath('.//div[@class="site-roster-card-image"]/@style').re_first(r'url\((.*?)\)')
        name = selector.xpath('.//div[@class="site-roster-card-content"]/h2/a/text()').extract_first().split()
        
        if len(name) > 3:
            first_name = " ".join(name)
            middle_name = ''
            last_name = ''
        else:
            first_name = name[0]
            middle_name = name[1] if len(name) == 3 else ''
            last_name = name[-1]
        
        phone_numbers = selector.xpath('.//div[@class="site-roster-card-content-title"]/span/a/text()').extract()
        email_url = selector.xpath('.//ul[@class="site-roster-icon-links"]/li/a/@href').re_first(r'mailto:(.*)')
        email = email_url if email_url and re.search(r'@', email_url) else ''

        return {
            "profile_url": f"https://www.alliebeth.com{profile_url}",
            "image_url": image_url,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
            "agent_phone_numbers": phone_numbers,
            "email": email
        }

    def save_to_db(self, data):
        """Save the parsed data to MongoDB, handling duplicates."""
        try:
            self.logger.info(f"Saving agent profile {data['first_name']} {data['last_name']} to MongoDB")
            self.collection.insert_one(data)
        except DuplicateKeyError:
            self.logger.warning(f"Duplicate profile found: {data['profile_url']}. Skipping entry.")

    def scrape(self):
        """Main function to scrape all data"""
        selector = self.fetch_data()
        agents = selector.xpath('//article')

        for agent_selector in agents:
            data = self.parse_agent(agent_selector)
            if data:
                self.save_to_db(data)

        self.logger.info(f"Finished scraping all agents from {self.url}.")

if __name__ == "__main__":
    scraper = AgentScraper(
        url='https://www.alliebeth.com/roster/Agents/0', 
        db_name='alliebeth_agent',
        collection_name='agents'
    )
    scraper.scrape()

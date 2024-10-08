from scrapy import Selector
import requests
import json
import logging
from pymongo import MongoClient, errors

logging.basicConfig(level=logging.INFO)

class RemaxCrawler:
    def __init__(self):
        self.start_url = 'https://www.remax.com/real-estate-agents'  
        self.mongo_client = MongoClient('mongodb://localhost:27017/')  
        self.db = self.mongo_client['remax_agents'] 
        self.collection = self.db['agents']  
        self.create_unique_index()

        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Referer': self.start_url,
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

    def create_unique_index(self):
        """Create a unique index on the collection."""
        self.collection.create_index(
            [
                ("first_name", 1),
                ("last_name", 1),
                ("email", 1)
            ],
            unique=True
        )
        logging.info("Unique index created on first_name, last_name, and email.")

    def fetch_page(self, url):
        """Fetch the page content from the given URL."""
        response = requests.get(url=url, headers=self.headers)
        return response.text if response.status_code == 200 else None

    def extract_agents(self, page_content):
        """Extract agent details from the page content."""
        selector = Selector(text=page_content)
        json_ld_script = selector.xpath("//script[@type='application/ld+json']/text()").extract_first()

        if json_ld_script:
            agents_data = json.loads(json_ld_script)
            return [self.parse_agent(agent, selector) for agent in agents_data]
        logging.warning("No JSON-LD script found.")
        return []

    def parse_agent(self, agent, selector):
        """Parse agent information and return a structured dictionary."""
        name_parts = agent.get('name', '').split()
        
        first_name = name_parts[0]
        middle_name = name_parts[1] if len(name_parts) > 2 else ''
        last_name = name_parts[-1] if name_parts else ''
        
        title = selector.xpath("//div[@class='d-agent-card-info']/div[1]/p/text()").extract_first(default='')
        office_name = selector.xpath("//h3[contains(@class, 'd-agent-card-office-name')]/text()").extract_first(default='')

        return {
            'first_name': first_name,
            'middle_name': middle_name,
            'last_name': last_name,
            'title': title,
            'office_name': office_name,
            'languages': [lang['name'] for lang in agent.get('knowsLanguage', [])],
            'image_url': agent.get('image', {}).get('url', ''),
            'address': agent.get('address', {}).get('streetAddress', ''),
            'city': agent.get('address', {}).get('addressLocality', ''),
            'state': agent.get('address', {}).get('addressRegion', ''),
            'country': 'USA',  
            'zipcode': agent.get('address', {}).get('postalCode', ''),
            'agent_phone_numbers': agent.get('telephone', ''),
            'email': agent.get('email', ''),
        }

    def save_agents_to_mongodb(self, agents_info):
        """Save the agent information to MongoDB."""
        for agent in agents_info:
            try:
                self.collection.insert_one(agent)
                logging.info(f"Inserted agent: {agent['first_name']} {agent['last_name']}")
            except errors.DuplicateKeyError:
                logging.warning(f"Duplicate agent found: {agent['first_name']} {agent['last_name']}. Skipping.")

    def crawl(self):
        """Start the crawling process."""
        page_number = 1

        while True:
            page_url = f"{self.start_url}?page={page_number}"
            page_content = self.fetch_page(page_url)

            if not page_content:
                logging.warning(f"No content found on page {page_number}. Stopping the crawl.")
                break
            
            agents_info = self.extract_agents(page_content)
            if not agents_info:
                logging.info(f"No agents found on page {page_number}. Stopping the crawl.")
                break
            
            self.save_agents_to_mongodb(agents_info)
            logging.info(f"Fetched agents from page {page_number}.")
            page_number += 1

if __name__ == "__main__":
    crawler = RemaxCrawler()
    crawler.crawl()

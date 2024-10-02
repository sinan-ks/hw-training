import requests
from scrapy import Selector
from pymongo import MongoClient, errors
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EWMAgentAddressParser:
    def __init__(self, db_name, collection_name):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        logging.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")
        
        self.headers = {
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://www.ewm.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

    def fetch_urls_from_db(self):
        """Fetch the website URLs from MongoDB."""
        return self.collection.find({"website": {"$ne": ""}}, {"_id": 1, "website": 1})

    def scrape_agent_address(self, website_url):
        """Extract address, city, state, zipcode, and description from the agent's webpage."""
        try:
            logging.info(f"Fetching page content from {website_url}")
            response = requests.get(website_url, headers=self.headers)
            response.raise_for_status()
            selector = Selector(text=response.text)
            
            address = selector.xpath("//div[@class='footer-top-left']//address/text()[1]").extract_first(default="").strip()
            
            # Extract city, state, and zipcode
            address_line_2 = selector.xpath("//div[@class='footer-top-left']//address/text()[2]").extract_first(default="").strip()
            city = state = zipcode = ""
            if address_line_2:
                parts = address_line_2.split(',')
                if len(parts) == 2:
                    city = parts[0].strip()
                    state_zip = parts[1].strip().split(' ')
                    if len(state_zip) >= 2:
                        state = state_zip[0].strip()
                        zipcode = state_zip[1].strip()
            
            description = selector.xpath("//*[contains(@class, 'about-widget-dscription')]//text()").extract_first(default="").strip()
            
            return {
                "address": address,
                "city": city,
                "state": state,
                "zipcode": zipcode,
                "description": description,
            }
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {website_url}: {e}")
            return None

    def update_agent_in_db(self, agent_id, agent_data):
        """Update agent data in MongoDB."""
        try:
            result = self.collection.update_one({"_id": agent_id}, {"$set": agent_data})
            if result.modified_count > 0:
                logging.info(f"Updated agent with _id: {agent_id}")
            else:
                logging.warning(f"No updates made for agent with _id: {agent_id}")
        except errors.PyMongoError as e:
            logging.error(f"Failed to update agent in DB: {e}")

    def parse_addresses(self):
        """Main function to fetch URLs, extract data, and update the collection."""
        logging.info("Starting the address parsing process...")
        agents = self.fetch_urls_from_db()
        
        for agent in agents:
            agent_id = agent["_id"]
            website_url = agent["website"]
            
            agent_data = self.scrape_agent_address(website_url)
            if agent_data:
                self.update_agent_in_db(agent_id, agent_data)

        logging.info("Address parsing process completed.")
        self.client.close()

if __name__ == "__main__":
    db_name = 'berkshare_hathway'
    collection_name = 'agents'

    parser = EWMAgentAddressParser(db_name, collection_name)
    parser.parse_addresses()

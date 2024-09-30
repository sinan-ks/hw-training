import requests
from scrapy import Selector
from pymongo import MongoClient
import logging
import re

class AgentDetailScraper:
    def __init__(self, db_name, collection_name, mongo_host='localhost', mongo_port=27017):
        self.client = MongoClient(mongo_host, mongo_port)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def fetch_agent_profiles(self):
        """Fetch profile URLs from MongoDB"""
        self.logger.info("Fetching agent profiles from MongoDB")
        return self.collection.find({}, {'profile_url': 1})

    def fetch_data(self, url):
        """Fetch agent page data from the profile URL"""
        self.logger.info(f"Fetching data from {url}")
        response = requests.get(url=url, headers=self.headers)
        return Selector(text=response.text)

    def parse_agent_details(self, selector):
        """Parse the agent details."""
        if selector is None:
            return {}  

        # Extract address, city, state, and zip code
        contact_info = selector.xpath("//div[@class='site-info-contact']/p[3]").extract_first(default='').strip()

        # If there are multiple <p> tags, we can join and find the relevant info
        if not contact_info:
            contact_info = ' '.join(selector.xpath("//div[@class='site-info-contact']/p/text()").extract()).strip()

        address = selector.xpath("//div[@class='site-info-contact']/p/b/text()").extract_first(default='').strip()

        city_state_zip = self.extract_city_state_zip(contact_info)
        description = self.parse_agents_descriptions(selector)
        office_name = selector.xpath("//div[@id='body-text-2-preview-16647-4913070']//strong/text()").extract_first(default='').strip()
        office_number = self.extract_office_number(selector)
        social_dict = self.extract_social_links(selector)
        title = self.extract_title(selector)
        website = selector.xpath("//ul[@class='no-bullet site-info-contact-icons']/li/a[contains(@href, 'http') and not(contains(@href, 'Contact'))]/@href").extract_first(default='').strip()

        return {
            "address": address,
            "city": city_state_zip[0],  
            "state": city_state_zip[1],
            "country": "United States",
            "zipcode": city_state_zip[2],
            "description": description,
            "languages": "",  
            "office_name": office_name,
            "office_number": [office_number],  
            "title": title,
            "website": website,
            "social": social_dict
        }

    def extract_city_state_zip(self, contact_info):
        """Helper method to parse city, state, and zipcode from contact info."""
        contact_info_clean = re.sub(r'<.*?>', '', contact_info).strip()

        match = re.search(r'(?P<city>[a-zA-Z\s]+)\s+(?P<state>[A-Z]{2})\s+(?P<zipcode>\d{5})', contact_info_clean)
        if match:
            city = match.group('city').strip().capitalize()  
            state = match.group('state').strip()
            zipcode = match.group('zipcode').strip()
            return city, state, zipcode
        else:
            return '', '', ''  

    def parse_agents_descriptions(self, selector):
        """Parse all agents' descriptions, handling both <p> and <ul> tags."""
        paragraphs = selector.xpath("//div[@class='site-about-column']//div[starts-with(@id, 'body-text-2-preview')]//p/text()").extract()
        list_items = selector.xpath("//div[@class='site-about-column']//div[starts-with(@id, 'body-text-2-preview')]//ul/li/text()").extract()

        all_text = paragraphs + list_items

        return ' '.join([text.strip() for text in all_text if text.strip()])

    def extract_office_number(self, selector):
        """Extract the office number."""
        office_number = ''
        
        office_info_raw = selector.xpath("//div[@class='site-info-contact']/p/text()").extract()

        for raw in office_info_raw:
            match = re.search(r'Office Phone:\s*\(?(\d{3})\)?[\s-]*(\d{3})[\s-]*(\d{4})', raw)
            if match:
                office_number = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
                break
        return office_number

    def extract_social_links(self, selector):
        """Extract social media links."""
        social_links = selector.xpath("//ul[@class='no-bullet site-bio-social']/li/a/@href").extract()

        social_dict = {
            "linkedin": '',
            "facebook": '',
            "twitter": '',
            "other_urls": []
        }

        for link in social_links:
            if 'linkedin' in link.lower():
                social_dict['linkedin'] = link
            elif 'facebook' in link.lower():
                social_dict['facebook'] = link
            elif 'twitter' in link.lower():
                social_dict['twitter'] = link
            else:
                social_dict['other_urls'].append(link)

        return social_dict

    def extract_title(self, selector):
        """Extract the title from the agent info."""
        title = selector.xpath("//div[@class='site-info-contact']/p[1]/text()").extract_first(default='').strip()

        if not title or title.lower() in ['mobile:', '']:
            return '' 
        
        return title

    def update_agent_details(self, profile_url, data):
        """Update the agent details in MongoDB."""
        self.logger.info(f"Updating agent details for {profile_url}")
        self.collection.update_one(
            {'profile_url': profile_url},
            {'$set': data},
            upsert=True
        )

    def scrape(self):
        """Main function to scrape all agent details."""
        agent_profiles = self.fetch_agent_profiles()

        for agent_profile in agent_profiles:
            profile_url = agent_profile['profile_url']
            self.logger.info(f"Processing {profile_url}")

            # Fetch the agent profile page and parse details
            selector = self.fetch_data(profile_url)
            agent_details = self.parse_agent_details(selector)

            self.update_agent_details(profile_url, agent_details)

        self.logger.info("Finished scraping and updating agent details.")


if __name__ == "__main__":
    scraper = AgentDetailScraper(
        db_name='alliebeth_agent',
        collection_name='agents'
    )
    scraper.scrape()

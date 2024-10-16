import requests
from scrapy import Selector
import re
import logging
from pipelines import MongoDBPipeline
from settings import AGENT_URL, HEADERS 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LongRealtyCrawler:
    def __init__(self, collection_name):
        self.pipeline = MongoDBPipeline(collection_name)

    def fetch_agents(self):
        logging.info("Fetching agents from %s", AGENT_URL)
        response = requests.get(url=AGENT_URL, headers=HEADERS)  
        if response.status_code == 200:
            selector = Selector(text=response.text)
            self.extract_agents(selector)
        else:
            logging.error("Failed to retrieve data: %s", response.status_code)

    def extract_agents(self, selector):
        AGENTS_XPATH = "//article[@class='rng-agent-roster-agent-card js-sort-item']"
        NAME_XPATH = ".//h1[@class='rn-agent-roster-name js-sort-name']/text()[1]"
        TITLE_XPATH = ".//h1[@class='rn-agent-roster-name js-sort-name']/span[@class='account-title']/text()"
        IMAGE_URL_XPATH = ".//img/@src"
        OFFICE_NAME_XPATH = ".//strong/text()"
        ADDRESS_XPATH = ".//strong/following-sibling::text()[1]"
        CITY_XPATH = ".//span[@class='js-sort-city']/text()"
        STATE_ZIP_XPATH = ".//p/strong/following-sibling::br[2]/following-sibling::text()[1]"
        WEBSITE_XPATH = ".//ul/li/a[contains(text(), 'Website')]/@href"
        LANGUAGES_XPATH = ".//p[contains(text(),'Speaks:')]/text()"
        EMAIL_XPATH = ".//ul/li/a[contains(text(), 'Email')]/text()"

        agents = selector.xpath(AGENTS_XPATH)
        logging.info("Found %d agents.", len(agents))
        for agent in agents:
            name = agent.xpath(NAME_XPATH).extract_first().strip()
            
            name_parts = name.split()
            if len(name_parts) > 3:
                first_name = ' '.join(name_parts)  
                middle_name = ''
                last_name = ''
            else:
                first_name = name_parts[0] if len(name_parts) > 0 else ''
                middle_name = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ''
                last_name = name_parts[-1] if len(name_parts) > 1 else ''

            title = agent.xpath(TITLE_XPATH).extract_first()
            image_url = agent.xpath(IMAGE_URL_XPATH).extract_first()
            office_name = agent.xpath(OFFICE_NAME_XPATH).extract_first()
            address = agent.xpath(ADDRESS_XPATH).extract_first()
            city = agent.xpath(CITY_XPATH).extract_first()
            state_zip = agent.xpath(STATE_ZIP_XPATH).extract_first()
            state = ""
            zipcode = ""

            if state_zip:
                cleaned_state_zip = re.sub(r'\s+', ' ', state_zip).strip()
                parts = cleaned_state_zip.split(' ')
                if len(parts) >= 3:
                    state = parts[1].strip()  
                    zipcode = parts[2].strip()  

            website = agent.xpath(WEBSITE_XPATH).extract_first()

            languages = agent.xpath(LANGUAGES_XPATH).extract_first()
            languages = languages.replace("Speaks: ", "").strip() if languages else ""
            languages = [lang.strip() for lang in languages.split(',')] if languages else []

            email = agent.xpath(EMAIL_XPATH).extract_first()
            email = email.replace(" Email ", "").strip() if email else ""

            if website:
                item = {}
                item["first_name"] = first_name
                item["middle_name"] = middle_name
                item["last_name"] = last_name
                item["office_name"] = office_name                
                item["title"] = title
                item["description"] = ""
                item["languages"] = languages                
                item["image_url"] = image_url
                item["address"] = address
                item["city"] = city
                item["state"] = state
                item["country"] = "United States"
                item["zipcode"] = zipcode
                item["office_phone_numbers"] = []  
                item["agent_phone_numbers"] = []              
                item["email"] = email
                item["website"] = website
                item["social"] = {}
                item["profile_url"] = website

                self.pipeline.insert_agent(item)

def main():
    collection_name = 'agents'  
    crawler = LongRealtyCrawler(collection_name)
    crawler.fetch_agents()

if __name__ == "__main__":
    main()
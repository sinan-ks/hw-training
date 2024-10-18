import requests
from scrapy import Selector
from settings import get_database, HEADERS, URL_COLLECTION_NAME, DATA_COLLECTION_NAME
from pipelines import MongoDBPipeline
import re

class AgentProfileParser:
    def __init__(self):
        self.db = get_database()
        self.pipeline = MongoDBPipeline(self.db, DATA_COLLECTION_NAME)
        self.profile_collection = self.db[URL_COLLECTION_NAME]

    def start(self):
        """Fetch profile URLs from MongoDB and parse each profile for additional details."""
        profiles = self.profile_collection.find()
        for profile in profiles:
            url = profile.get("profile_url")
            if url:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:
                    self.parse_profile(response, url)
                else:
                    print(f"Failed to fetch profile at {url}, status code: {response.status_code}")

    def parse_profile(self, response, url):
        """Parse the agent's profile page and extract details."""
        title = ""
        description = ""
        website = ""
        social = {}

        response = Selector(text=response.text)

        # XPATH
        NAME_XPATH = "//h1[@class='gl-profile__name']/text()"
        OFFICE_NAME_XPATH = "//p[@class='gl-profile__org']/text()"
        LANGUAGES_XPATH = "//div[@id='content-container-3']/ul/li[@class='gl-profile__detail']/span[2]/text()"
        IMAGE_URL_XPATH = "//div[@class='gl-profile__user']//img/@src"
        ADDRESS_XPATH = "//li[@class='gl-profile__detail address']/text()"
        CITY_STATE_ZIP_COUNTRY_XPATH = "//li[@class='gl-profile__detail address']/descendant-or-self::text()[preceding-sibling::br]"
        OFFICE_PHONE_NUMBERS_XPATH = "//a[@aria-label='Call Office']/span/text()"
        AGENT_PHONE_NUMBERS_XPATH = "//a[@aria-label='Call Agent']/span/text()"
        EMAIL_XPATH = "//li[@class='gl-profile__detail profile']//a[contains(@href, 'mailto:')]/span/text()"
        
        name = response.xpath(NAME_XPATH).extract_first('').strip()
        office_name = response.xpath(OFFICE_NAME_XPATH).extract_first('').strip()
        languages = response.xpath(LANGUAGES_XPATH).extract()
        image_url = response.xpath(IMAGE_URL_XPATH).extract_first('').strip()
        address = response.xpath(ADDRESS_XPATH).extract_first('').strip()   
        address_full = response.xpath(CITY_STATE_ZIP_COUNTRY_XPATH).extract_first('').strip()
        office_phone_numbers = response.xpath(OFFICE_PHONE_NUMBERS_XPATH).extract() 
        agent_phone_numbers = response.xpath(AGENT_PHONE_NUMBERS_XPATH).extract()             
        email = response.xpath(EMAIL_XPATH).extract_first(default='').strip()

        # CLEANING
        """Splits name into first_name, middle_name and last_name."""
        name_parts = name.split()
        if len(name_parts) > 3:
            first_name = ' '.join(name_parts)  
            middle_name = ''
            last_name = ''
        else:
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            middle_name = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ''
            last_name = name_parts[-1] if len(name_parts) > 1 else ''

        """Splits an address into city, state, zip code, and country components."""
        pattern = r"^(.*?),\s*([A-Z]+)?\s*(\d{4,6})?,?\s*(.*)$"
        match = re.match(pattern, address_full)

        if match:
            city = match.group(1).strip() if match.group(1) else ""
            state = match.group(2).strip() if match.group(2) else ""
            zipcode = match.group(3).strip() if match.group(3) else ""
            country = match.group(4).strip() if match.group(4) else ""
        else:
            city = ""
            state = ""
            zipcode = ""
            country = ""

        # DATA INSERTION      
        item = {}
        item["first_name"] = first_name
        item["middle_name"] = middle_name
        item["last_name"] = last_name
        item["office_name"] = office_name
        item["title"] = title
        item["description"] = description
        item["languages"] = languages
        item["image_url"] = image_url
        item["address"] = address
        item["city"] = city
        item["state"] = state
        item["zipcode"] = zipcode
        item["country"] = country
        item["office_phone_numbers"] = office_phone_numbers
        item["agent_phone_numbers"] = agent_phone_numbers
        item["email"] = email
        item["website"] = website
        item["social"] = social
        item["profile_url"] = url              

        # Insert into MongoDB
        self.pipeline.insert(item)

def main():
    parser = AgentProfileParser() 
    parser.start()

if __name__ == "__main__":
    main()
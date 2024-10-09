import json
import requests
from scrapy import Selector
import logging
from settings import mongo_client, DB_NAME, COLLECTION_URLS, HEADERS
from pipelines import ProfilePipeline

class ProfileParser:
    def __init__(self):
        self.db = mongo_client[DB_NAME]
        self.collection = self.db[COLLECTION_URLS]
        self.pipeline = ProfilePipeline()

    def fetch_urls_and_profile_pages(self):
        """Fetch all URLs from MongoDB and their corresponding profile page content."""
        urls = [doc['url'] for doc in self.collection.find({}, {'url': 1})]

        for url in urls:
            try:
                response = requests.get(url, headers=HEADERS, timeout=10)
                if response.status_code == 200:
                    yield url, response.text
                else:
                    logging.warning(f"Failed to fetch {url}. Status code: {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Error fetching {url}: {e}")
                continue

    def extract_profile_data(self, page_content):
        """Extract profile data from the page content using XPath constants."""

        TITLE_XPATH = "//h2[@data-testid='d-text' and contains(@class, 'd-bio-header-title')]/text()"
        DESCRIPTION_XPATH = "//h3[text()='About Me']/following-sibling::p//text()"
        SOCIAL_LINKS_XPATH = "//div/p[@data-testid='d-text' and strong[text()='Social Media']]/following-sibling::a[@data-testid='d-link']/@href"
        MOBILE_PHONE_XPATH = "//div[@class='d-information-container']/p[strong[contains(text(), 'Mobile')]]/text()[1]"
        OFFICE_PHONE_XPATH = "//div[@class='d-information-container']/p[strong[contains(text(), 'Direct')]]/text()[1]"
        JSON_LD_XPATH = "//script[@type='application/ld+json']/text()"

        selector = Selector(text=page_content)
        
        json_ld_script = selector.xpath(JSON_LD_XPATH).extract_first()

        social = {
            'linkedin': '',
            'facebook': '',
            'twitter': '',
            'other_urls': [],
        }

        if json_ld_script:
            agent_data = json.loads(json_ld_script)

            title = selector.xpath(TITLE_XPATH).extract_first('').strip()
            title = ' '.join(title.split()) 
            
            description = selector.xpath(DESCRIPTION_XPATH).extract()
            description = ' '.join([desc.strip() for desc in description if desc.strip()]) 
            description = ' '.join(description.split())  

            languages = agent_data.get('knowsLanguage', [])
            cleaned_languages = [lang for lang in languages if lang] 

            social_links = selector.xpath(SOCIAL_LINKS_XPATH).extract()

            for link in social_links:
                if 'linkedin.com' in link:
                    social['linkedin'] = link
                elif 'facebook.com' in link:
                    social['facebook'] = link
                elif 'twitter.com' in link:
                    social['twitter'] = link
                else:
                    social['other_urls'].append(link)

            agent_phone_numbers = selector.xpath(MOBILE_PHONE_XPATH).extract_first('').strip()
            office_phone_numbers = selector.xpath(OFFICE_PHONE_XPATH).extract_first('').strip()

            name_parts = agent_data['name'].split()
        
            if len(name_parts) > 3:
                first_name = ' '.join(name_parts)
                middle_name = ''
                last_name = ''
            else:
                first_name = name_parts[0] if len(name_parts) > 0 else ''
                middle_name = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ''
                last_name = name_parts[-1] if len(name_parts) > 1 else ''

            if all(not value for value in social.values()):
                social = {}

            return {
                'first_name': first_name,
                'middle_name': middle_name,
                'last_name': last_name,
                'office_name': agent_data.get('subOrganization', ''),
                'title': title,
                'description': description,
                'languages': cleaned_languages,
                'image_url': agent_data.get('image', ''),
                'address': agent_data['address'].get('streetAddress', ''),
                'city': agent_data['address'].get('addressLocality', ''),
                'state': agent_data['address'].get('addressRegion', ''),
                'country': 'United States',
                'zipcode': agent_data['address'].get('postalCode', ''),
                'office_phone_numbers': [office_phone_numbers] if office_phone_numbers else [],
                'agent_phone_numbers': [agent_phone_numbers] if agent_phone_numbers else [],
                'email': agent_data.get('email', ''),
                'website': agent_data.get('url', ''),
                'social': social,
                'profile_url': '',
            }
        else:
            logging.warning("No JSON-LD script found.")
            return None

    def parse_profiles(self):
        """Main method to parse profiles from the URLs."""
        for url, page_content in self.fetch_urls_and_profile_pages():
            profile_data = self.extract_profile_data(page_content)
            if profile_data:
                self.pipeline.save_profile_to_mongodb(profile_data, url)
            else:
                logging.warning(f"No profile data extracted from {url}.")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the parser...")
    parser = ProfileParser()
    parser.parse_profiles()
    logging.info("Parser finished.")

if __name__ == "__main__":
    main()
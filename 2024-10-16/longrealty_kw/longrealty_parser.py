import requests
from scrapy import Selector
import logging
import time
from pipelines import MongoDBPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LongRealtyProfileParser:
    def __init__(self, collection_name):
        self.pipeline = MongoDBPipeline(collection_name)

    def fetch_profile(self, profile_url, retries=3, delay=5):
        """
        Attempts to fetch the profile page. Retries if it fails to connect.
        Logs the profile URL if it's unreachable after max retries.
        """
        for attempt in range(retries):
            try:
                response = requests.get(profile_url, verify=False, timeout=10)
                if response.status_code == 200:
                    return response 
                else:
                    logging.error(f"Non-200 status code for {profile_url}: {response.status_code}")
                    return None
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout for {profile_url}. Retrying in {delay} seconds...")
                time.sleep(delay)
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching profile {profile_url}: {e}")
                return None

        logging.error(f"Max retries exceeded for {profile_url}. Adding to failed profiles list.")
        with open('failed_profiles.log', 'a') as log_file:
            log_file.write(f"{profile_url}\n")
        return None

    def update_agent_details(self):
        agents = self.pipeline.collection.find({"profile_url": {"$ne": None}})

        AGENT_PHONE_NUMBERS_XPATH = "//div[@class='site-column quarter']//li[contains(text(), 'Direct:')]/text()"
        DESCRIPTION_XPATH = "//div[contains(@class, 'site-home-page-content-text') or contains(@class, 'site-home-page-about-text') or contains(@class, 'site-cms-text')]/p//text()"
        SOCIAL_MEDIA_LINKS_XPATH = "//ul[contains(@class, 'footer-social')]/li/a/@href"

        for agent in agents:
            profile_url = agent.get("profile_url")
            if not profile_url:
                logging.warning("No profile_url for agent: %s", agent.get("first_name", "Unknown"))
                continue

            logging.info("Fetching details from profile: %s", profile_url)
            response = self.fetch_profile(profile_url)
            if response:
                selector = Selector(text=response.text)
                
                agent_phone_numbers = []
                phone_number = selector.xpath(AGENT_PHONE_NUMBERS_XPATH).extract_first()
                if phone_number:
                    agent_phone_numbers.append(phone_number.replace("Direct:", "").strip())
                
                description = selector.xpath(DESCRIPTION_XPATH).extract()
                if description:
                    description = " ".join([desc.strip() for desc in description])
                else:
                    description = ""  
                
                social_links = selector.xpath(SOCIAL_MEDIA_LINKS_XPATH).extract()
                social = {
                    'facebook': '',
                    'linkedin': '',
                    'twitter': '',
                    'other_urls': []
                }

                if social_links:
                    for link in social_links:
                        if 'linkedin.com' in link:
                            social['linkedin'] = link
                        elif 'facebook.com' in link:
                            social['facebook'] = link
                        elif 'twitter.com' in link:
                            social['twitter'] = link
                        else:
                            social['other_urls'].append(link)
                else:
                    social = {}  

                self.pipeline.update_agent(agent["_id"], {
                    "agent_phone_numbers": agent_phone_numbers,  
                    "description": description,
                    "social": social 
                })
                logging.info("Updated agent: %s with phone numbers, description, and social links.", agent.get("first_name", "Unknown"))
            else:
                logging.error("Failed to retrieve profile page: %s", profile_url)

def main():
    collection_name = 'agents' 
    parser = LongRealtyProfileParser(collection_name)
    parser.update_agent_details()

if __name__ == "__main__":
    main()

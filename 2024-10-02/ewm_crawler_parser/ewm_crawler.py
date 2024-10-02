import requests
from scrapy import Selector
from pymongo import MongoClient, errors
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EWMAgentCrawler:
    def __init__(self, start_url, db_name, collection_name):
        self.start_url = start_url
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.collection.create_index('profile_url', unique=True)
        logging.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")
        
        self.headers = {
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://www.ewm.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

    def scrape_page(self, url):
        """ Fetch agent data from a given page. """
        logging.info(f"Fetching page content from {url}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            selector = Selector(text=response.text)

            agents = selector.xpath("//div[contains(@class,'listing-box')]")
            if agents:
                logging.info(f"Found {len(agents)} agents on page {url}")
                return [self.extract_agent_data(agent) for agent in agents]
            else:
                logging.warning(f"No agents found on page {url}")
                return []
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return []

    def extract_agent_data(self, agent):
        """ Extract data for a single agent. """
        try:
            name = agent.xpath(".//div[contains(@class,'listing-box-title')]/h2/a/text()").extract_first()
            title = agent.xpath(".//div[contains(@class,'listing-box-title')]/h3/text()").extract_first()

            if name:
                name = name.strip()
                name_parts = name.split()
                if len(name_parts) > 3:
                    first_name = name
                    middle_name = ''
                    last_name = ''
                else:
                    first_name = name_parts[0] if len(name_parts) > 0 else ''
                    middle_name = name_parts[1] if len(name_parts) > 2 else ''
                    last_name = name_parts[-1] if len(name_parts) > 1 else ''
            else:
                first_name = middle_name = last_name = ''

            title = title.strip() if title else ''

            image_url = agent.xpath(".//div[contains(@class,'listing-box-image')]/img/@src").extract_first()

            office_phone_numbers = [phone.strip() for phone in agent.xpath(".//div[contains(@class,'listing-box-content')]/p/a[3]/text()").extract() if phone.strip()]
            agent_phone_numbers = [phone.strip() for phone in agent.xpath(".//div[contains(@class,'listing-box-content')]/p/a[5]/text()").extract() if phone.strip()]

            profile_url = agent.xpath(".//div[contains(@class,'listing-box-image')]/a/@href").extract_first()

            email = agent.xpath(".//div[contains(@class,'listing-box-content')]//a[contains(@href, 'emailme')]/@href").extract_first()
            email = email.split(':')[-1] if email and '@' in email else ''

            social_links = agent.xpath(".//ul[@class='listing-box-social']/li/a/@href").extract()

            social = {
                'linkedin': '',
                'facebook': '',
                'twitter': '',
                'other_urls': [],
            }

            for link in social_links:
                if 'linkedin.com' in link:
                    social['linkedin'] = link
                elif 'facebook.com' in link:
                    social['facebook'] = link
                elif 'twitter.com' in link:
                    social['twitter'] = link
                else:
                    social['other_urls'].append(link)

            website = agent.xpath(".//p/a[contains(@href, 'ewm.com')]/@href").extract_first()  
            website = website.strip() if website else ''

            office_name = agent.xpath(".//div[contains(@class,'listing-box-title')]/h6/text()").extract_first()
            office_name = office_name.strip() if office_name else ''

            languages = agent.xpath(".//div[contains(@class,'listing-box-content')]/p//a[contains(@href, '#')]/i[contains(@class, 'fa-comments-o')]/following-sibling::text()").extract()
            languages = [lang.replace('Speaks:', '').strip() for lang in languages]

            if languages:
                languages = [lang.strip() for lang in ', '.join(languages).split(',')]
            else:
                languages = []  

            if not profile_url:
                logging.warning("No profile URL found, skipping agent.")
                return None  

            return {
                'first_name': first_name,
                'middle_name': middle_name,
                'last_name': last_name,
                'title': title,
                'image_url': image_url,
                'office_phone_numbers': office_phone_numbers,
                'agent_phone_numbers': agent_phone_numbers,
                'profile_url': profile_url,
                'email': email,
                'social': social,
                'website': website,
                'office_name': office_name,
                'languages': languages,
                'country': 'United States'
            }
        except Exception as e:
            logging.error(f"Failed to extract agent data: {e}")
            return None

    def insert_agent(self, agent_data):
        """ Insert agent data into the database only if the website is not empty. """
        if agent_data:
            if agent_data.get('website'):
                try:
                    self.collection.insert_one(agent_data)
                    logging.info(f"Inserted agent into DB: {agent_data['first_name']}")
                except errors.DuplicateKeyError:
                    logging.info(f"Duplicate agent found and skipped: {agent_data['profile_url']}")
                    existing_agent = self.collection.find_one({
                        'first_name': agent_data['first_name'],
                        'middle_name': agent_data['middle_name'],
                        'last_name': agent_data['last_name'],
                        'website': ''
                    })
                    if existing_agent:
                        self.collection.update_one(
                            {'_id': existing_agent['_id']},
                            {'$set': {'website': agent_data['website']}}
                        )
                        logging.info(f"Updated existing agent with website: {agent_data['first_name']}")
                except errors.PyMongoError as e:
                    logging.error(f"Failed to insert agent into DB: {e}")
            else:
                logging.info(f"Skipped agent with empty website: {agent_data['first_name']}")

    def crawl(self):
        """ Start the crawling process from the start URL, following pagination links. """
        logging.info("Starting crawl process...")
        current_page = 1
        while True:
            current_url = f"{self.start_url}?page={current_page}"
            agents_data = self.scrape_page(current_url)
            if not agents_data:
                logging.info("No more agents found, stopping crawl.")
                break
            
            for agent in agents_data:
                self.insert_agent(agent)

            current_page += 1
            time.sleep(1)  

        logging.info("Crawl process completed.")
        self.client.close()

if __name__ == "__main__":
    start_url = 'https://www.ewm.com/agents.php'
    db_name = 'berkshare_hathway'
    collection_name = 'agents'

    crawler = EWMAgentCrawler(start_url, db_name, collection_name)
    crawler.crawl()

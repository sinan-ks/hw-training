import re
import requests
import logging
from settings import collection_urls, HEADERS

class RemaxCrawler:
    def __init__(self):
        self.sitemap_url = 'https://www.remax.com/agents-1.xml'
    
    def crawl(self):
        """Fetch sitemap, extract URLs, and save to MongoDB."""
        try:
            response = requests.get(self.sitemap_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            agent_urls = re.findall(r'<loc>(.*?)</loc>', response.text)
            self.save_urls(agent_urls)
        except requests.RequestException as e:
            logging.error(f"Failed to fetch the sitemap. Error: {e}")
    
    def save_urls(self, urls):
        """Save URLs to MongoDB, limit to 3000."""
        count = 0
        for url in urls:
            try:
                collection_urls.insert_one({"url": url})
                logging.info(f"Inserted URL: {url}")
                count += 1
                if count >= 2500:
                    break
            except Exception as e:
                logging.warning(f"Error inserting URL: {url}. Error: {e}")

        logging.info(f"Total URLs processed: {count}")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the crawler...")
    crawler = RemaxCrawler()
    crawler.crawl()
    logging.info("Crawler finished.")
    
if __name__ == "__main__":
    main()
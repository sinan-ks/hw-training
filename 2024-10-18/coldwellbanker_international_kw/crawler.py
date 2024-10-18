import requests
from settings import get_database, HEADERS, URL_COLLECTION_NAME, COUNTRIES, BASE_URL, AGENT_URL_BASE
from pipelines import MongoDBPipeline

class ColdwellBankerScraper:
    def __init__(self):
        self.db = get_database()
        self.pipeline = MongoDBPipeline(self.db, URL_COLLECTION_NAME)

    def start(self, country):
        """Fetch agents for a specific country and store their UUIDs in MongoDB."""
        url = BASE_URL.format(country)
        response = requests.get(url=url, headers=HEADERS)

        if response.status_code == 200:
            print(f"Fetching agents from {country}")
            agents = response.json().get("pins", [])

            for agent in agents[:300]:
                profile_url = AGENT_URL_BASE + agent["uuid"]
                agent_data = {"profile_url": profile_url}
                self.pipeline.insert(agent_data)
        else:
            print(f"Failed to fetch data for {country}, status code: {response.status_code}")

    def scrape_agents(self):
        """Main function to scrape agents for all countries."""
        for country in COUNTRIES:
            self.start(country)

def main():
    scraper = ColdwellBankerScraper()
    scraper.scrape_agents()

if __name__ == "__main__":
    main()

from pymongo import MongoClient

# Database settings
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'remax_agents'
COLLECTION_URLS = 'agent_urls'
COLLECTION_PROFILES = 'profile_details'

# HTTP headers for requests
HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://www.remax.com/real-estate-agents',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
}

# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection_urls = db[COLLECTION_URLS]
collection_profiles = db[COLLECTION_PROFILES]

# Ensure unique index on URLs
collection_urls.create_index("url", unique=True)

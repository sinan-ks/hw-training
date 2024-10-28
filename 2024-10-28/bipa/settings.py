import pytz
from datetime import datetime, timedelta

# Client and project details
CLIENT_NAME = "tcs_aldi"
COUNTRY_NAME = "austria"
FREQUENCY = "weekly"
PROJECT = "bipa"
TIMEZONE_STR = "Asia/Kolkata"

# MongoDB collection names
URL_COLLECTION_NAME = f"{PROJECT}_url"
DATA_COLLECTION_NAME = f"{PROJECT}_data"
URL_404_COLLECTION_NAME = f"{PROJECT}_url_404"

# Generate the MongoDB database name 
timezone = pytz.timezone(TIMEZONE_STR)
today = datetime.now(timezone)

if today.weekday() == 3:  # If today is Thursday
    iteration = today.strftime('%Y_%m_%d')
else:
    days_ahead = (3 - today.weekday() + 7) % 7
    next_thursday = today + timedelta(days=days_ahead)
    iteration = next_thursday.strftime('%Y_%m_%d')

EXTRACTION_DATE = today.strftime("%Y-%m-%d")

# MongoDB details
MONGO_DB_NAME = f"{CLIENT_NAME}_{COUNTRY_NAME}_{FREQUENCY}_{iteration}"
MONGO_URI = 'mongodb://localhost:27017/'

# Output File
FILE_NAME = f"DataHut_AT_{PROJECT}_PriceExtractions_{iteration}.CSV"

# API credentials
HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
}

# Client ID and Refresh Token
CLIENT_ID = '2f55a356-1392-4de0-9cdf-f74f0666f43d'
REFRESH_TOKEN = 'bEAZoKb70zXVpooeStuUvn5HYHA5-XsiK10Sz0MhlQU'

# API URLs
TOKEN_URL = 'https://www.bipa.at/mobify/proxy/api/shopper/auth/v1/organizations/f_ecom_aaft_prd/oauth2/token'
CATEGORY_URL = 'https://www.bipa.at/resolve-url/navigation'
BASE_URL = 'https://www.bipa.at/mobify/proxy/api/search/shopper-search/v1/organizations/f_ecom_aaft_prd/product-search'

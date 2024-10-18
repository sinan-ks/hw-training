from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

CLIENT_NAME = "kw"
PROJECT = "coldwellbankerinternational"
FREQUENCY = "monthly"
TIMEZONE = "Asia/Kolkata"

# MongoDB collection names
URL_COLLECTION_NAME = f"{PROJECT}_url"
DATA_COLLECTION_NAME = f"{PROJECT}_data"

def get_database():
    """Sets up and returns the MongoDB connection."""
    timezone = pytz.timezone(TIMEZONE)
    datetime_obj = datetime.now(timezone)
    next_month_date = datetime_obj + relativedelta(months=1)
    YYY_MM_DD = f"{next_month_date.year}_{next_month_date.month:02d}_05"
    db_name = f"{CLIENT_NAME}_{FREQUENCY}_{YYY_MM_DD}"

    client = MongoClient('localhost', 27017)
    db = client[db_name]
    return db, YYY_MM_DD

# Base URLs for scraping
BASE_URL = "https://www.coldwellbankerinternational.com/api/search?country={}&market=agents&e=1729145809"
AGENT_URL_BASE = "https://www.coldwellbankerinternational.com/agent/"

HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
    'content-type': 'application/json',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
}

# Countries for scraping
COUNTRIES = [
    "ar", "aw", "bm", "vg", "ca", "ky", "cn", "co", "cr", "cw",
    "cy", "do", "eg", "fr", "de", "gh", "gd", "gt", "in", "id",
    "ie", "it", "jm", "ke", "mt", "mx", "mc", "an", "pa", "pt"
]



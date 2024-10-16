
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME_TEMPLATE = 'kw_monthly_{year}_{next_month:02d}_05'
AGENT_URL = 'https://www.longrealty.com/roster/agents/0'

HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
    'Referer': 'https://www.longrealty.com/roster/agents',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
}



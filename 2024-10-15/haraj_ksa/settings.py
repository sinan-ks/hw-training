from pymongo import MongoClient

BASE_URL = 'https://haraj.com.sa/'
API_URL = 'https://graphql.haraj.com.sa/?queryName=posts&clientId=GqbIJwRY-LLsX-nZTm-tTsi-XQwJ7Ys2BtZrv3&version=N9.0.38%20,%208/26/2024/'

HEADERS = {
    'accept': '*/*',
    'content-type': 'application/json',
    'origin': BASE_URL,
    'referer': BASE_URL,
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
}

DB_CLIENT = MongoClient('mongodb://localhost:27017/')
COLLECTION_NAME = 'haraj_property_details'
DB = DB_CLIENT['propertyfinder_monthly_2024_10']
COLLECTION = DB[COLLECTION_NAME]
COLLECTION.create_index('id', unique=True)

INPUTS = [
    {
        "url": "https://haraj.com.sa/tags/%D8%A8%D9%8A%D9%88%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Houses",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "بيوت للايجار",
        "category_url": "/tags/%D8%A8%D9%8A%D9%88%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%A7%D8%AF%D9%88%D8%A7%D8%B1%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Floors",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "ادوار للايجار",
        "category_url": "/tags/%D8%A7%D8%AF%D9%88%D8%A7%D8%B1%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D9%85%D8%B2%D8%A7%D8%B1%D8%B9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Farms",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "مزارع للبيع",
        "category_url": "/tags/%D9%85%D8%B2%D8%A7%D8%B1%D8%B9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D9%81%D9%84%D9%84%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Villas",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "فلل للايجار",
        "category_url": "/tags/%D9%81%D9%84%D9%84%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B4%D8%A7%D9%84%D9%8A%D9%87%D8%A7%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Chalets",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "شاليهات للبيع",
        "category_url": "/tags/%D8%B4%D8%A7%D9%84%D9%8A%D9%87%D8%A7%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B9%D9%85%D8%A7%D8%B1%D9%87%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Buildings",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "عماره للايجار",
        "category_url": "/tags/%D8%B9%D9%85%D8%A7%D8%B1%D9%87%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D9%85%D8%AD%D9%84%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Shops",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "محلات للايجار",
        "category_url": "/tags/%D9%85%D8%AD%D9%84%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D9%85%D8%AD%D9%84%D8%A7%D8%AA%20%D9%84%D9%84%D8%AA%D9%82%D8%A8%D9%8A%D9%84",
        "property_type": "Shops",
        "sub_category_1": "Commercial",
        "category": "waiver",
        "slug": "محلات للتقبيل",
        "category_url": "/tags/%D9%85%D8%AD%D9%84%D8%A7%D8%AA%20%D9%84%D9%84%D8%AA%D9%82%D8%A8%D9%8A%D9%84",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B4%D8%A7%D9%84%D9%8A%D9%87%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Chalets",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "شاليهات للايجار",
        "category_url": "/tags/%D8%B4%D8%A7%D9%84%D9%8A%D9%87%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B9%D9%85%D8%A7%D8%B1%D8%A9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Buildings",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "عمارة للبيع",
        "category_url": "/tags/%D8%B9%D9%85%D8%A7%D8%B1%D8%A9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%A7%D8%B1%D8%A7%D8%B6%D9%8A%20%D8%AA%D8%AC%D8%A7%D8%B1%D9%8A%D8%A9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Commercial Land",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "اراضي تجارية للبيع",
        "category_url": "/tags/%D8%A7%D8%B1%D8%A7%D8%B6%D9%8A%20%D8%AA%D8%AC%D8%A7%D8%B1%D9%8A%D8%A9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%A8%D9%8A%D9%88%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Houses",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "بيوت للبيع",
        "category_url": "/tags/%D8%A8%D9%8A%D9%88%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B4%D9%82%D9%82%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Apartments",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "شقق للبيع",
        "category_url": "/tags/%D8%B4%D9%82%D9%82%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D9%81%D9%84%D9%84%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Villas",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "فلل للبيع",
        "category_url": "/tags/%D9%81%D9%84%D9%84%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%B4%D9%82%D9%82%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Apartments",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "شقق للايجار",
        "category_url": "/tags/%D8%B4%D9%82%D9%82%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%A7%D8%B1%D8%A7%D8%B6%D9%8A%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Land",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "اراضي للبيع",
        "category_url": "/tags/%D8%A7%D8%B1%D8%A7%D8%B6%D9%8A%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    # # #     ============================ new category ========================= # # #
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%85%D9%83%D8%A7%D8%AA%D8%A8%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Offices",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "مكاتب للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%85%D9%83%D8%A7%D8%AA%D8%A8%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B3%D8%AA%D9%88%D8%AF%D8%B9%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Warehouses",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "مستودعات للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B3%D8%AA%D9%88%D8%AF%D8%B9%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B3%D8%AA%D9%88%D8%AF%D8%B9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Warehouses",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "مستودع للبيع",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B3%D8%AA%D9%88%D8%AF%D8%B9%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D8%AF%D9%88%D8%B1%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Role",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "دور للبيع",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D8%AF%D9%88%D8%B1%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D8%A7%D8%B3%D8%AA%D8%B1%D8%A7%D8%AD%D8%A7%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Resthouses",
        "sub_category_1": "Residential",
        "category": "sale",
        "slug": "استراحات للبيع",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D8%A7%D8%B3%D8%AA%D8%B1%D8%A7%D8%AD%D8%A7%D8%AA%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D8%A7%D8%B3%D8%AA%D8%B1%D8%A7%D8%AD%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Resthouses",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "استراحات للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D8%A7%D8%B3%D8%AA%D8%B1%D8%A7%D8%AD%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B2%D8%B1%D8%B9%D8%A9%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Farms",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "مزرعة للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%B2%D8%B1%D8%B9%D8%A9%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%83%D9%85%D8%A8%D8%A7%D9%88%D9%86%D8%AF%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
        "property_type": "Compounds",
        "sub_category_1": "Commercial",
        "category": "sale",
        "slug": "كمباوند للبيع",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%83%D9%85%D8%A8%D8%A7%D9%88%D9%86%D8%AF%20%D9%84%D9%84%D8%A8%D9%8A%D8%B9",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%83%D9%85%D8%A8%D8%A7%D9%88%D9%86%D8%AF%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Compounds",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "كمباوند للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%83%D9%85%D8%A8%D8%A7%D9%88%D9%86%D8%AF%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%AE%D9%8A%D9%85%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Camps",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "مخيمات للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%85%D8%AE%D9%8A%D9%85%D8%A7%D8%AA%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D9%82%D8%A7%D8%B9%D8%A9%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Hall",
        "sub_category_1": "Commercial",
        "category": "rent",
        "slug": "قاعة للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D9%82%D8%A7%D8%B9%D8%A9%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
    {
        "url": "https://haraj.com.sa/tags/%D8%AC%D8%AF%D9%87_%D8%BA%D8%B1%D9%81%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
        "property_type": "Rooms",
        "sub_category_1": "Residential",
        "category": "rent",
        "slug": "غرف للايجار",
        "category_url": "/tags/%D8%AC%D8%AF%D9%87_%D8%BA%D8%B1%D9%81%20%D9%84%D9%84%D8%A7%D9%8A%D8%AC%D8%A7%D8%B1",
    },
]

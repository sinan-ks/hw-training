import requests
from mongoengine import connect
from mongoengine import NotUniqueError
from settings import MONGO_DB_NAME, MONGO_URI, HEADERS, CLIENT_ID, REFRESH_TOKEN, URL_COLLECTION_NAME, TOKEN_URL, CATEGORY_URL, BASE_URL
from items import ProductURL

class BipaCrawler:
    def __init__(self):
        connect(MONGO_DB_NAME, host=MONGO_URI)
        ProductURL._meta['collection'] = URL_COLLECTION_NAME

    def start(self):
        global REFRESH_TOKEN
        token_url = TOKEN_URL
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': REFRESH_TOKEN,
            'client_id': CLIENT_ID,
        }        
        response = requests.post(token_url, headers=HEADERS, data=data)

        if response.status_code != 200:
            print(f"Failed to get access token, status code: {response.status_code}, response: {response.text}")
            if "invalid refresh_token" in response.text:
                print("The refresh token is invalid. Please obtain a new refresh token.")
            return None

        token_data = response.json()
        access_token = token_data.get("access_token")
        new_refresh_token = token_data.get("refresh_token")

        # Update the refresh token in settings
        REFRESH_TOKEN = new_refresh_token 
        HEADERS.update({"Authorization": f"Bearer {access_token}"})        

        category_url = CATEGORY_URL
        response = requests.get(category_url, headers=HEADERS)

        if response.status_code != 200:
            print(f"Failed to fetch categories, status code: {response.status_code}")
            return

        data = response.json()
        main_categories = data[0].get('childCategories', [])

        for category in main_categories:
            element_id = category.get('elementId', '')
            if element_id.startswith("cgid_"):
                refine_value = element_id.replace("cgid_", "")
                print(f"Fetching product URLs for category: {refine_value}")

                product_urls = []
                offset = 0
                limit = 20
                base_url = BASE_URL

                while len(product_urls) < 150:
                    params = {
                        'siteId': 'AT',
                        'refine': f'cgid={refine_value}',
                        'sort': 'Neu eingetroffen',
                        'currency': 'EUR',
                        'locale': 'de-AT',
                        'expand': 'availability,images',
                        'offset': str(offset),
                        'limit': str(limit),
                    }
                    response = requests.get(base_url, headers=HEADERS, params=params)
                    if response.status_code == 200:
                        response_data = response.json()
                        hits = response_data.get("hits", [])
                        if not hits:
                            break
                        for hit in hits:
                            if hit.get("orderable", False):
                                product_id = hit.get("productId")
                                product_urls.append(f"https://www.bipa.at/p/{product_id}")
                        offset += limit
                    else:
                        print(f"Failed to fetch products for category {refine_value}, status code: {response.status_code}")
                        break

                for url in product_urls:
                    product_url = ProductURL(url=url)
                    try:
                        product_url.save()  
                        print(f"Inserted URL: {url}")                    
                    except NotUniqueError:
                        print(f"Duplicate URL encountered: {url}") 
                        
                print(f"Inserted {len(product_urls)} product URLs for category: {refine_value}.")                


def main():
    scraper = BipaCrawler()
    scraper.start()

if __name__ == "__main__":
    main()

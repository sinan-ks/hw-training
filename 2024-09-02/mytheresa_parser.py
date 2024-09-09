import requests
from parsel import Selector
from pymongo import MongoClient
import json
import re

class MytheresaParser:
    def __init__(self, db_name, collection_name, output_collection_name):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.output_collection = self.db[output_collection_name]

    def clean_price(self, price):
        if price:
            # Remove non-numeric characters except commas
            price = re.sub(r'[^\d,]', '', price)
            return f"€ {price}"
        return None

    def extract_price(self, selector, class_name):
        # Extract all price elements
        prices = selector.xpath(f'//span[contains(@class, "{class_name}")]//span[contains(@class, "pricing__prices__price")]/text()').getall()
        # Clean the prices by stripping whitespace
        prices = [price.strip() for price in prices if price.strip()]
        
        # Determine the correct price
        if len(prices) > 1:
            price = prices[1]
        else:
            price = prices[0] if prices else None

        return self.clean_price(price)

    def parse_product_page(self, product_url):
        response = requests.get(product_url)
        if response.status_code == 200:
            selector = Selector(text=response.text)
            
            breadcrumbs = selector.xpath('//div[contains(@class, "breadcrumb__item")]/a/text()').getall()
            image_url = selector.xpath('//div[@class="photocarousel__items"]//img[contains(@class, "product__gallery__carousel__image")]/@src').get()
            brand = selector.xpath('//div[contains(@class, "product__area__branding__designer")]//a/text()').get()
            product_name = selector.xpath('//div[contains(@class, "product__area__branding__name")]/text()').get()

            listing_price = self.extract_price(selector, "pricing__prices__value--original")
            offer_price = self.extract_price(selector, "pricing__prices__value--discount")
            discount = selector.xpath('//span[contains(@class, "pricing__info__percentage")]/text()').get()
            
            product_id = selector.xpath('//div[contains(@class, "accordion__body__content")]//ul//li').re_first(r'Item number:\s*(\w+)')
            sizes = selector.xpath('//div[contains(@class, "sizeitem")]//span[contains(@class, "sizeitem__label")]/text()').getall()

            # Convert description list to a single string
            description = selector.xpath('//div[contains(@class, "accordion__body__content")]//ul//li/text()').getall()
            description = " ".join(description).strip()

            # Avoiding the main image and duplicates
            other_images = selector.xpath('//div[contains(@class, "swiper-wrapper")]//div[contains(@class, "swiper-slide")]//img[contains(@class, "product__gallery__carousel__image")]/@src').getall()
            other_images = list(set([url for url in other_images if url != image_url]))  

            # Structure the extracted data
            product_data = {
                "breadcrumbs": breadcrumbs,
                "image_url": image_url,
                "brand": brand,
                "product_name": product_name,
                "listing_price": listing_price,
                "offer_price": offer_price,
                "discount": discount,
                "product_id": product_id,
                "sizes": sizes,
                "description": description,
                "other_images": other_images,
            }

            return product_data

    def parse_all_products(self):
        products = []
        for record in self.collection.find():
            product_url = record['url']
            product_data = self.parse_product_page(product_url)
            if product_data:
                # Check if the product already exists in the database using product_id
                existing_product = self.output_collection.find_one({"product_id": product_data["product_id"]})
                if not existing_product:
                    print(json.dumps(product_data, indent=4, ensure_ascii=False))
                    # Save product details to the database
                    result = self.output_collection.insert_one(product_data)

                    # Add the _id field as a string to the product_data dictionary
                    product_data["_id"] = str(result.inserted_id)

                    # Collect product data for saving to JSON 
                    products.append(product_data)
        return products

if __name__ == "__main__":
    db_name = 'mytheresa'
    collection_name = 'product_urls'
    output_collection_name = 'product_details'

    parser = MytheresaParser(db_name, collection_name, output_collection_name)
    products = parser.parse_all_products()

    # Save parsed data to a JSON file
    with open('mytheresa_shoes_data.json', 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=4)
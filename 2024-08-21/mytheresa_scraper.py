import requests
from parsel import Selector
from urllib.parse import urljoin
import json
import re

base_url = 'https://www.mytheresa.com/int_en/men/shoes.html'
product_urls = []

def get_product_links(page_url):
    response = requests.get(page_url)
    if response.status_code == 200:
        selector = Selector(text=response.text)
        
        # Extract product URLs
        product_links = selector.css('div.item a::attr(href)').getall()
        for link in product_links:
            full_url = urljoin(page_url, link)
            product_urls.append(full_url)

        # Check for the next page and crawl it
        next_page = selector.xpath('//a[contains(@class, "pagination__item") and contains(@class, "icon__next")]/@href').get()
        if next_page:
            full_next_page_url = urljoin(page_url, next_page)
            get_product_links(full_next_page_url)

def clean_price(price):
    if price:
        # Remove non-numeric characters except commas
        price = re.sub(r'[^\d,]', '', price)
        return f"€ {price}"
    return None

def extract_price(selector, class_name):
    # Extract all price elements
    prices = selector.xpath(f'//span[contains(@class, "{class_name}")]//span[contains(@class, "pricing__prices__price")]/text()').getall()
    # Clean the prices by stripping whitespace
    prices = [price.strip() for price in prices if price.strip()]
    
    # Determine the correct price
    if len(prices) > 1:
        price = prices[1]
    else:
        price = prices[0] if prices else None

    # Clean the price
    return clean_price(price)

def parse_product_page(product_url):
    response = requests.get(product_url)
    if response.status_code == 200:
        selector = Selector(text=response.text)
        
        breadcrumbs = selector.xpath('//div[contains(@class, "breadcrumb__item")]/a/text()').getall()
        image_url = selector.xpath('//div[@class="photocarousel__items"]//img[contains(@class, "product__gallery__carousel__image")]/@src').get()
        brand = selector.xpath('//div[contains(@class, "product__area__branding__designer")]//a/text()').get()
        product_name = selector.xpath('//div[contains(@class, "product__area__branding__name")]/text()').get()

        listing_price = extract_price(selector, "pricing__prices__value--original")
        offer_price = extract_price(selector, "pricing__prices__value--discount")
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

def remove_duplicates(urls):
    return list(set(urls))

def scrape_mytheresa_shoes():
    get_product_links(base_url)
    
    # Remove duplicate URLs
    unique_product_urls = remove_duplicates(product_urls)
    
    products = []

    for product_url in unique_product_urls:
        product_data = parse_product_page(product_url)
        if product_data:
            products.append(product_data)
            print(json.dumps(product_data, indent=4, ensure_ascii=False))
    
    # Save to a JSON file
    with open('mytheresa_shoes_data.json', 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=4)
    
if __name__ == "__main__":
    scrape_mytheresa_shoes()

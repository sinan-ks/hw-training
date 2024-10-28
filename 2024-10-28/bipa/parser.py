import requests
from mongoengine import connect
from scrapy import Selector
from settings import MONGO_DB_NAME, MONGO_URI, HEADERS, URL_COLLECTION_NAME, URL_404_COLLECTION_NAME, DATA_COLLECTION_NAME, EXTRACTION_DATE
from items import ProductURL, ProductItem, ProductURL404
from pipelines import MongoPipeline
import json 
import bleach
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BipaParser:
    def __init__(self):
        connect(MONGO_DB_NAME, host=MONGO_URI)
        ProductURL._meta['collection'] = URL_COLLECTION_NAME
        ProductItem._meta['collection'] = DATA_COLLECTION_NAME
        ProductURL404._meta['collection'] = URL_404_COLLECTION_NAME
        self.pipeline = MongoPipeline()

    def start(self):
        logging.info("Starting the scraping process...")        
        product_urls = ProductURL.objects()

        for product_url in product_urls:
            url = product_url.url
            logging.info(f"Fetching URL: {url}")
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 404:
                logging.warning(f"URL not found (404): {url}")
                self.pipeline.insert_404(url)
                continue
            elif response.status_code != 200:
                logging.error(f"Failed to fetch product page: {url}, status code: {response.status_code}")
                continue

            logging.info(f"Successfully fetched: {url}")
            self.parse_product_details(response, url)

    def parse_product_details(self, response, url):
        """Parse the relevant fields from the product data."""
        selector = Selector(text=response.text)

        # XPATH
        SCRIPT_XPATH = "//script[contains(text(), '__PRELOADED_STATE__')]/text()"
        BASE_XPATH = "//p[@class='chakra-text css-16mom77']/text()[preceding-sibling::span[contains(text(), '{label}')]]"
        INSTRUCTIONS_XPATH = BASE_XPATH.format(label="Verwendungshinweis")
        STORAGE_INSTRUCTIONS_XPATH = BASE_XPATH.format(label="Aufbewahrungshinweis")
        PREPARATIONINSTRUCTIONS_XPATH = BASE_XPATH.format(label="Zubereitungsanweisung")
        COUNTRY_OF_ORIGIN_XPATH = BASE_XPATH.format(label="Herkunftsländer")
        ALLERGENS_XPATH = BASE_XPATH.format(label="Enthält")
        NUTRITIONS_XPATH = BASE_XPATH.format(label="VEGAN') or contains(text(), 'VEGETARIAN")
        NUTRITIONAL_INFO_1_XPATH = BASE_XPATH.format(label="Haushaltsportion")
        NUTRITIONAL_INFO_2_XPATH = BASE_XPATH.format(label="Unzubereitet")
        NUTRITIONAL_INFO_3_XPATH = "//div[contains(@class, 'chakra-table__container')]//table"
        LABELLING_XPATH = BASE_XPATH.format(label="Kennzeichnung")
        PACKAGING_XPATH = "//div[@class='chakra-stack css-1igwmid']//p[contains(text(), '(Verpackungskennzeichen)')]/text()"
        DIMENSIONS_XPATH = "//div[@class='chakra-stack css-2xph3x']//h3[contains(text(), 'Abmessungen')]/following-sibling::div/p"
        NETCONTENT_XPATH = BASE_XPATH.format(label="Nettogehalt")
        INGREDIENTS_XPATH = BASE_XPATH.format(label="Zutaten")

        json_ld_script = selector.xpath(SCRIPT_XPATH).extract_first()
        instructions = selector.xpath(INSTRUCTIONS_XPATH).extract()
        storage_instructions = selector.xpath(STORAGE_INSTRUCTIONS_XPATH).extract()
        preparationinstructions = selector.xpath(PREPARATIONINSTRUCTIONS_XPATH).extract_first()
        country_of_origin = selector.xpath(COUNTRY_OF_ORIGIN_XPATH).extract_first("")
        allergens = selector.xpath(ALLERGENS_XPATH).extract_first("")
        nutritions = selector.xpath(NUTRITIONS_XPATH).extract()
        nutritional_info_1 = selector.xpath(NUTRITIONAL_INFO_1_XPATH).extract_first("")
        nutritional_info_2 = selector.xpath(NUTRITIONAL_INFO_2_XPATH).extract_first("")
        nutritional_info_3 = selector.xpath(NUTRITIONAL_INFO_3_XPATH)
        labelling = selector.xpath(LABELLING_XPATH).extract_first("")
        packaging = selector.xpath(PACKAGING_XPATH).extract_first("")
        dimentions = selector.xpath(DIMENSIONS_XPATH)
        netcontent = selector.xpath(NETCONTENT_XPATH).extract_first("")
        ingredients = selector.xpath(INGREDIENTS_XPATH).extract_first("")

        if not json_ld_script:
            logging.warning(f"No JSON-LD script found for URL: {url}")
            return None

        json_data = json.loads(json_ld_script)
        product_data = json_data.get("__PRELOADED_STATE__", {}).get("pageProps", {}).get("product", {})
        
        # SCRIPT
        unique_id = product_data.get("c_articleNo", "")
        product_name = f"{product_data.get('c_kundenbezeichnung', [''])[0]} {product_data.get('c_content', '')}"
        brand = product_data.get("brand", "")
        grammage_quantity_unit = product_data.get("c_content", "").split()
        breadcrumb = product_data.get("c_categories", [])
        regular_price = float(product_data.get("price", ""))  
        selling_price = float(product_data.get("c_displayedPrice", ""))  
        saving_value = product_data.get("c_saving", "")
        price_badges_value = product_data.get("c_pricebadges", [""])[0].upper()  
        price_per_unit = product_data.get("c_basePrice", "")
        bulletpoints = product_data.get("c_allBulletpoints", [])
        long_description = product_data.get("longDescription", "")
        color = product_data.get("c_farbe", [""])[0] 
        size = product_data.get("c_size", "")
        rating = product_data.get("c_bvAverageRating", "")
        review = product_data.get("c_bvReviewCount", "")
        images = product_data.get('imageGroups', [{}])[0].get('images', [])
        retail_limit = product_data.get("inventory", {}).get("ats", 0)
        
        # CLEANING
        """ Remove Extra Whitespace In title """
        title = product_name.strip()

        """ Extract the grammage quantity and unit from the content. """
        grammage_quantity = grammage_quantity_unit[0] if len(grammage_quantity_unit) > 0 else ""
        grammage_unit = grammage_quantity_unit[1] if len(grammage_quantity_unit) > 1 else ""

        """ Create a breadcrumb from c_categories with proper formatting. """
        if breadcrumb:
            first_category = breadcrumb[0]  
            breadcrumb_parts = [part.capitalize() for part in first_category.split('-')]
            breadcrumb = "Startseite > " + " > ".join(breadcrumb_parts)
            product_hierarchy = ["Startseite"] + breadcrumb_parts
        else:
            product_hierarchy = ["Startseite"]

        while len(product_hierarchy) < 7:
            product_hierarchy.append("")                   

        """ Format saving value to a negative Euro amount if available. """
        if saving_value:
            promotion_description = f"-€{abs(float(saving_value)):.2f}"
        else:
            promotion_description = ""
        
        price_was = ""
        promotion_price = ""

        # Handle AKTION and DAUERTIEFPREIS conditions
        if "AKTION" in price_badges_value:
            # Condition 1: AKTION found
            if selling_price == regular_price:
                regular_price = ""
                selling_price = promotion_price = selling_price
                price_was = ""
                promotion_description = "AKTION"
            else:
                price_was = regular_price
                promotion_description = f"{promotion_description}, AKTION"
                regular_price = regular_price
                selling_price = promotion_price = selling_price

        elif "DAUERTIEFPREIS" in price_badges_value:
            # Condition 4: DAUERTIEFPREIS found
            if promotion_description:  
                regular_price = selling_price  
                price_was = promotion_price = ""
            else:
                regular_price = selling_price = regular_price
                price_was = promotion_price = ""
                promotion_description = ""

        elif not promotion_description:
            # Condition 2: AKTION not found and promotion_description is empty
            if selling_price == regular_price:
                regular_price = selling_price = selling_price
                price_was = promotion_price = ""
                promotion_description = ""
            else:
                price_was = regular_price
                regular_price = regular_price
                selling_price = promotion_price = selling_price
                promotion_description = ""

        else:
            # Condition 3: promotion_description is not empty
            if selling_price == regular_price:
                regular_price = selling_price = selling_price
                price_was = promotion_price = ""
            else:
                regular_price = selling_price = regular_price
                promotion_price = selling_price
                price_was = regular_price

        """ Normalize the price per unit by replacing commas with periods if necessary. """
        if isinstance(price_per_unit, str):
            price_per_unit = price_per_unit.replace(',', '.')

        """ Collect variant values and names of the variation attributes from the product data. """
        variant_values_dict = {}
        variation_names = []

        if 'variants' in product_data:
            for variant in product_data['variants']:
                variation_values = variant.get('variationValues', {})
                for key, value in variation_values.items():
                    if key not in variant_values_dict:
                        variant_values_dict[key] = set()  
                    variant_values_dict[key].add(value) 

        if 'variationAttributes' in product_data:
            for attribute in product_data['variationAttributes']:
                attribute_name = attribute.get('name', '')
                if attribute_name:
                    variation_names.append(attribute_name)

        variation_dict = {}
        for i, name in enumerate(variation_names):
            key = list(variant_values_dict.keys())[i] if i < len(variant_values_dict.keys()) else None
            if key and key in variant_values_dict:
                variation_dict[name] = list(variant_values_dict[key]) 

        if not variation_dict:
            variation_dict = ""

        """ Construct the product description from bullet points and long description. """
        description = f"{','.join(bulletpoints)}{long_description}"
        cleaned_description = bleach.clean(description, tags=[], strip=True)
        final_description = ' '.join(cleaned_description.split())

        """ Join and clean the instructions text. """
        instructions_text = ' '.join(' '.join(instructions).split()).strip() if instructions else "" 
        storage_instructions_text = ' '.join(storage_instructions).strip() if storage_instructions else ""
        preparationinstructions_text = ' '.join(preparationinstructions).strip() if preparationinstructions else ""

        """ Extract and format nutrition information. """
        nutrition_info = {}

        for nutrition in nutritions:
            if 'Vegan' in nutrition:
                nutrition_info['VEGAN'] = nutrition
            elif 'Vegetarisch' in nutrition:
                nutrition_info['VEGETARIAN'] = nutrition

        nutritions_text = ', '.join([f"{key}: {value}" for key, value in nutrition_info.items()])

        """ Extracts portion text and nutritional data from tables, storing it in a dictionary."""
        portion_text = nutritional_info_1
        if not portion_text:
            portion_text = nutritional_info_2
        
        tables = nutritional_info_3
        data_dict = {}

        for table in tables:
            rows = table.xpath('.//tbody/tr')
            headers = table.xpath('.//thead/tr/th/text()').extract()
            if len(headers) > 1:
                unit = headers[1].strip()

            for row in rows:
                nutrient = row.xpath('td[1]/text()').extract_first("").strip()
                value = row.xpath('td[2]/text()').extract_first("").strip()

                if nutrient and value:
                    key = f"{nutrient}_{unit}".replace(' ', '_')
                    data_dict[key] = value.strip()

        if data_dict:  
            if portion_text:
                nutrition_data = {
                    portion_text: data_dict
                }
            else:
                nutrition_data = data_dict
        else:
            nutrition_data = "" 

        """ Extract and format product dimensions. """
        dimensions_info = {}

        for dimension in dimentions:
            label = dimension.xpath("span/text()").extract_first().strip()  
            value = dimension.xpath("text()").extract_first().strip()      
            dimensions_info[label] = value  

        dimensions_text = ', '.join([f"{key}: {value}" for key, value in dimensions_info.items()])

        """ Extract images from the imageGroups and process them for file names and URLs """
        base_image_url = "https://www.bipa.at/on/demandware.static/-/Sites-catalog/de_AT/original/"
        image_files = []
        image_urls = []

        for image in images[:6]:  
            file_name = image.get("link", "").split('/')[-1]
            image_url = f"{base_image_url}{file_name}"
            image_files.append(file_name)
            image_urls.append(image_url)
    
        image_files.extend([""] * (6 - len(image_files)))
        image_urls.extend([""] * (6 - len(image_urls)))

        """ Join and clean the ingredients text. """
        ingredients_text = ingredients.strip()
        ingredients_text = ' '.join(ingredients_text.split())

        # DATA INSERTION 
        item = {}
        item["unique_id"] = unique_id
        item["competitor_name"] = "bipa"
        item["store_name"] = ""
        item["store_addressline1"] = ""
        item["store_addressline2"] = ""
        item["store_suburb"] = ""
        item["store_state"] = ""
        item["store_postcode"] = ""
        item["store_addressid"] = ""
        item["extraction_date"] = EXTRACTION_DATE
        item["product_name"] = title
        item["brand"] = brand
        item["brand_type"] = ""
        item["grammage_quantity"] = grammage_quantity
        item["grammage_unit"] = grammage_unit
        item["drained_weight"] = ""
        item["producthierarchy_level1"] = product_hierarchy[0]
        item["producthierarchy_level2"] = product_hierarchy[1]
        item["producthierarchy_level3"] = product_hierarchy[2]
        item["producthierarchy_level4"] = product_hierarchy[3]
        item["producthierarchy_level5"] = product_hierarchy[4]
        item["producthierarchy_level6"] = product_hierarchy[5]
        item["producthierarchy_level7"] = product_hierarchy[6]
        item["regular_price"] = regular_price
        item["selling_price"] = selling_price
        item["price_was"] = price_was
        item["promotion_price"] = promotion_price
        item["promotion_valid_from"] = ""
        item["promotion_valid_upto"] = ""
        item["promotion_type"] = ""
        item["percentage_discount"] = ""
        item["promotion_description"] = promotion_description
        item["package_sizeof_sellingprice"] = ""
        item["per_unit_sizedescription"] = ""
        item["price_valid_from"] = ""
        item["price_per_unit"] = price_per_unit
        item["multi_buy_item_count"] = ""
        item["multi_buy_items_price_total"] = ""
        item["currency "] = "EUR"
        item["breadcrumb"] = breadcrumb
        item["pdp_url"] = url
        item["variants"] = variation_dict
        item["product_description"] = final_description
        item["instructions"] = instructions_text
        item["storage_instructions"] = storage_instructions_text
        item["preparationinstruction"] = preparationinstructions_text
        item["instructionforuse"] = instructions_text
        item["country_of_origin"] = country_of_origin
        item["allergens"] = allergens
        item["age_of_the_product"] = ""
        item["age_recommendations"] = ""
        item["flavour"] = ""
        item["nutritions"] = nutritions_text
        item["nutritional_information"] = nutrition_data
        item["vitamins"] = ""
        item["labelling"] = labelling
        item["grade"] = ""
        item["region"] = ""
        item["packaging"] = packaging
        item["receipies"] = ""
        item["processed_food"] = ""
        item["barcode"] = ""
        item["frozen"] = ""
        item["chilled"] = ""
        item["organictype"] = ""
        item["cooking_part"] = ""
        item["handmade"] = ""
        item["max_heating_temperature"] = ""
        item["special_information"] = ""
        item["label_information"] = ""
        item["dimensions"] = dimensions_text
        item["special_nutrition_purpose"] = ""
        item["feeding_recommendation"] = ""
        item["warranty"] = ""
        item["color"] = color
        item["model_number"] = ""
        item["material"] = ""
        item["usp"] = ""
        item["dosage_recommendation"] = ""
        item["tasting_note"] = ""
        item["food_preservation"] = ""
        item["size"] = size
        item["rating"] = rating
        item["review"] = review
        item["file_name_1"] = image_files[0]
        item["image_url_1"] = image_urls[0]
        item["file_name_2"] = image_files[1]
        item["image_url_2"] = image_urls[1]
        item["file_name_3"] = image_files[2]
        item["image_url_3"] = image_urls[2]
        item["file_name_4"] = image_files[3]
        item["image_url_4"] = image_urls[3]
        item["file_name_5"] = image_files[4]
        item["image_url_5"] = image_urls[4]
        item["file_name_6"] = image_files[5]
        item["image_url_6"] = image_urls[5]
        item["competitor_product_key"] = ""
        item["fit_guide"] = ""
        item["occasion"] = ""
        item["material_composition"] = ""
        item["style"] = ""
        item["care_instructions"] = ""
        item["heel_type"] = ""
        item["heel_height"] = ""
        item["upc"] = ""
        item["features"] = ""
        item["dietary_lifestyle"] = ""
        item["manufacturer_address"] = ""
        item["importer_address"] = ""
        item["distributor_address"] = ""
        item["vinification_details"] = ""
        item["recycling_information"] = ""
        item["return_address"] = ""
        item["alcohol_by_volume"] = ""
        item["beer_deg"] = ""       
        item["netcontent"] = netcontent
        item["netweight"] = ""
        item["site_shown_uom"] = title
        item["ingredients"] = ingredients_text
        item["random_weight_flag"] = ""           
        item["instock"] = True 
        item["promo_limit"] = ""   
        item["product_unique_key"] = unique_id+"P"
        item["multibuy_items_pricesingle"] = ""
        item["perfect_match"] = ""
        item["servings_per_pack"] = ""
        item["warning"] = ""
        item["suitable_for"] = ""
        item["standard_drinks"] = ""
        item["environmental"] = ""
        item["grape_variety"] = ""
        item["retail_limit"] = retail_limit

        logging.info(f"Parsed data for {product_name.strip()} (ID: {unique_id})")        
        if unique_id:  
            self.pipeline.insert_data(item)
            logging.info(f"Inserted data for product ID: {unique_id}")
        else:
            logging.warning(f"Skipping product with empty unique ID for URL: {url}")              


def main():
    parser = BipaParser()
    parser.start()

if __name__ == "__main__":
    main()

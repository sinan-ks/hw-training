from mongoengine import connect
from settings import MONGO_DB_NAME, MONGO_URI
from items import ProductURL404, ProductItem

class MongoPipeline:
    def __init__(self):
        connect(MONGO_DB_NAME, host=MONGO_URI)
        print(f"Connected to MongoDB: {MONGO_DB_NAME}")

    def insert_404(self, url):
        """Insert a URL that returned a 404 response into the MongoDB collection."""
        product_url_404 = ProductURL404(url=url)
        product_url_404.save()

    def insert_data(self, item):
        """Insert product data into the MongoDB collection."""
        product_data = ProductItem(**item)
        product_data.save()

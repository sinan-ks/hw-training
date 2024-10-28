from mongoengine import Document, StringField, DynamicDocument
from settings import DATA_COLLECTION_NAME

class ProductURL(Document):
    url = StringField(required=True, unique=True)  

    meta = {
        'indexes': [
            {'fields': ['url'], 'unique': True}  
        ]
    }

class ProductURL404(Document):
    url = StringField(required=True)

class ProductItem(DynamicDocument):
    meta = {
        'collection': DATA_COLLECTION_NAME  
    }    
    unique_id = StringField(required=True)
    competitor_name = StringField(required=True)
    product_name = StringField(required=True)

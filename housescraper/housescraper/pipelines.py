# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import mysql.connector
from itemadapter import ItemAdapter

class HousescraperPipeline:
    def __init__(self, db_user, db_password, db_host, db_name):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
           db_user=crawler.settings.get('MYSQL_USER'),
           db_password=crawler.settings.get('MYSQL_PASSWORD'),
           db_host=crawler.settings.get('MYSQL_HOST'),
           db_name=crawler.settings.get('MYSQL_NAME')
        )

    def open_spider(self, spider):
        self.conn = mysql.connector.connect(
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            database=self.db_name
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        sql = """
            INSERT INTO flats (
                property_id, Landmark, title, price, area_sqft,
                property_name, image_url, area, status,
                furnishing, facing, floor, overlook, url, latitude, longitude,
                addressLocality, addressRegion, Beds, bathroom, balcony,
                parking, amenities, url_overview,NearbyLocality, rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)
        """
        values = (
            item.get('property_id'),
            item.get('Landmark'),
            item.get('title'),
            item.get('price'),
            item.get('area_sqft'),
            item.get('property_name'),
            item.get('image_url'),
            item.get('area'),
            item.get('status'),
            item.get('furnishing'),
            item.get('facing'),
            item.get('floor'),
            item.get('overlook'),
            item.get('url'),
            item.get('latitude'),
            item.get('longitude'),
            item.get('addressLocality'),
            item.get('addressRegion'),
            item.get('Beds'),
            item.get('bathroom'),
            item.get('balcony'),
            item.get('parking'),
            ', '.join(item.get('amenities', [])),  # Join list of amenities to string
            item.get('url_overview'),
            ', '.join(item.get('NearbyLocality', [])),  # Join list of NearbyLocality to string
            ', '.join(item.get('rating', []))  # Join list of ratings to string
        )
        try:
            self.cursor.execute(sql, values)
            self.conn.commit()
        except mysql.connector.Error as err:
            print("Error:", err)
            self.conn.rollback()
        
        return item



    
    # def process_item(self,item,spider):
    #     cleaned_item=self.cleaned_item(item)
    #     validated_item=self.validate_item(cleaned_item)

    # def cleaned_item(self,item):
    #     cleaned_item = {key: self.clean_value(value) for key, value in item.items()}
    #     return cleaned_item
    
    # def clean_and_validate_data(data):
    #     cleaned_data={}

    #     for key,values in data.items():
    #         if 
#Cleaning the data 






# data=[
# {'Beds': '2',
#  'Landmark': 'Prabhadevi, Mumbai',
#  'NearbyLocality': ['Government Technical High School Prabhadevi',
#                     "St Francis Xavier's High School",
#                     'Ramanand Arya Dav College',
#                     'Kanjurmarg Railway Station',
#                     'R City Mall',
#                     'Huma Mall',
#                     'Sahakari Bhandar',
#                     'DMart Ready',
#                     'DMart Ready',
#                     'Vijay Sales',
#                     'Krishna Nagar Parel, Mumbai',
#                     'Bhawani Sankar, Mumbai',
#                     'Gokhale Road, Mumbai',
#                     'Elphinstone, Mumbai',
#                     'Century Mills, Mumbai',
#                     'Dadar West, Mumbai',
#                     'P K Das & Associates',
#                     'Lodha Ithink Techno Campus',
#                     'Godrej And Boyce Industry Estate',
#                     'Khed Galli',
#                     'Chhatrapati Shivaji Maharaj International Airport'],
#  'addressLocality': 'Prabhadevi',
#  'addressRegion': 'Mumbai',
#  'amenities': ['Power Back Up',
#                'Lift',
#                'Rain Water Harvesting',
#                'Club House',
#                'Swimming Pool',
#                'Gymnasium'],
#  'area_sqft': '35625 per sqft ',
#  'balcony': '0',
#  'bathroom': '2',
#  'carpet_area': '481 sqft',
#  'facing': 'West',
#  'floor': '10 out of 10',
#  'furnishing': 'Unfurnished',
#  'image_url': 'https://img.staticmb.com/mbphoto/property/cropped_images/2024/Mar/19/Photo_h300_w450/71908911_1_PropertyImage1710848892723_300_450.jpg',
#  'latitude': '19.0158585103406',
#  'longitude': '72.8295254470428',
#  'overlook': 'Garden/Park, Main Road',
#  'parking': '0',
#  'price': '2.28 Cr ',
#  'property_id': '71908911',
#  'property_name': 'Eon One',
#  'rating': ['Environment 4.5/5',
#             'Commuting 4.2/5',
#             'Places of Interest 4.4/5',
#             'Prabhadevi, Mumbai 4.4/5'],
#  'status': 'Ready to Move',
#  'super_area': None,
#  'title': '2 BHK Flat for Sale in Prabhadevi, Mumbai',
#  'url': 'https://www.magicbricks.com/propertyDetails/2-BHK-640-Sq-ft-Multistorey-Apartment-FOR-Sale-Prabhadevi-in-Mumbai&id=4d423731393038393131'}

# ]
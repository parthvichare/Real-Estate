# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import mysql.connector
from itemadapter import ItemAdapter
import numpy as np

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
    
    def treat_floor(self,x):
        x=str(x)
        x=x.replace('Ground','0')
        x=x.split(' ')[0].split('/')[0].strip()
        if x.isdigit():
            return int(x)
        else:
            return None
    
    def treat_price(self, x):
        if x == 'Call for price':
            return np.nan
        elif type(x) == float:
            return x
        elif x[1] == 'Lac':
            return round(float(x[0]) / 100, 2)
        else:
            return round(float(x[0]), 2)
        


    def process_item(self, item, spider):
        price_value=item.get('price')
        if price_value:
            # Split the price before applying treat_price
            price_split = price_value.split(' ')
            
            # Apply the treat_price logic to the split price
            treated_price = self.treat_price(price_split)
        else:
            treated_price = np.nan  # or handle this case as needed
        sql = """
            INSERT INTO flats (
                property_id, Landmark, title, price, area_sqft,
                property_name, image_url, area, status,
                furnishing, facing, floor, overlook, url, latitude, longitude,
                addressLocality, addressRegion, Beds, bathroom, balcony,
                parking, amenities, url_overview,flat_details,NearbyLocality, rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s)
        """
        values = (
            item.get('property_id'),
            item.get('Landmark'),
            item.get('title'),
            treated_price,
            item.get('area_sqft'),
            item.get('property_name'),
            item.get('image_url'),
            item.get('area'),
            item.get('status'),
            item.get('furnishing'),
            item.get('facing'),
            self.treat_floor(item.get('floor')), 
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
            ', '.join(map(str, item.get('flat_details', []))),
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
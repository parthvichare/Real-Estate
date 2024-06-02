# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class HouseItem(scrapy.Item):
    # Define the fields for your item here like:
    property_id = scrapy.Field()
    Landmark = scrapy.Field()        # Is this field necessary? It's not used in the SQL insert query.
    title = scrapy.Field()
    price = scrapy.Field()
    area_sqft = scrapy.Field()
    property_name = scrapy.Field()
    image_url = scrapy.Field()
    area = scrapy.Field()
    status = scrapy.Field()
    furnishing = scrapy.Field()
    facing = scrapy.Field()
    floor = scrapy.Field()
    overlook = scrapy.Field()  # Add the overlooking field here
    url = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    addressLocality = scrapy.Field()
    addressRegion = scrapy.Field()
    Beds = scrapy.Field()
    bathroom = scrapy.Field()
    balcony = scrapy.Field()
    parking = scrapy.Field()
    url_overview=scrapy.Field()
    flat_details=scrapy.Field()
    amenities = scrapy.Field()

    # New fields for property info
    NearbyLocality = scrapy.Field()
    rating = scrapy.Field()



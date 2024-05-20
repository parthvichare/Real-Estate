
# # # Get a url-link,latitude,longitude,address

# # Data to extract from the next_url
# # Feature(Ammenties):- lift,park,garden,wifi-connectivuty,vastu-compliant
# # rating:- ['Environment4 out of 5', 'Lifestyle4 out of 5', 'Connectivity4 out of 5', 'Safety4 out of 5']
# # Nearby loction hospitals,institues,colleges



# import scrapy
# import json
# import csv
# from housescraper.items import HouseItem

# class MySpider(scrapy.Spider):
#     name = 'housespider'
#     allowed_domains = ['magicbricks.com']
#     start_urls = ['https://www.magicbricks.com/flats-in-mumbai-for-sale-pppfs']
# #     # start_urls=['https://www.magicbricks.com/flats-in-new-delhi-for-sale-pppfs']
# #     # start_urls=['https://www.magicbricks.com/flats-for-rent-in-mumbai-pppfr']
# #     # start_urls=['https://www.magicbricks.com/independent-house-for-rent-in-mumbai-pppfr']
# #     # start_urls=['https://www.magicbricks.com/property-for-rent/residential-paying-guest?cityName=Mumbai&BudgetMin=4000&BudgetMax=6000']
#     items = []


#     def __init__(self):
#         self.houses = {'data': []} 
#         self.items=[]
#         self.page_count=0

#     def start_requests(self):
#         for url in self.start_urls:
#             yield scrapy.Request(url=url, callback=self.parse)

#     def parse_flat(self, flat):
#         title = flat.css('h2.mb-srp__card--title::text').get()
#         price = flat.css('div.mb-srp__card__price--amount::text').get()
#         sqft = flat.css('div.mb-srp__card__price--size::text').get()
#         property_name = flat.css('a.mb-srp__card__society--name::text').get()
#         image_url = flat.css('div.mb-srp__card__photo__fig img::attr(data-src)').get()

#         carpet_area = flat.css('[data-summary="carpet-area"] .mb-srp__card__summary--value::text').get()
#         super_area =flat.css('[data-summary="super-area"] .mb-srp__card__summary--value::text').get()
#         status = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Status")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
#         furnishing = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Furnishing")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
#         facing = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "facing")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
#         floor = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Floor")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
#         overlooking = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "overlooking")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        
#         return {
#             'title': title,
#             'image_url': image_url,
#             'property_name': property_name,
#             'price': price,
#             'area_sqft': sqft,
#             'carpet_area': carpet_area,
#             'super-area':super_area,
#             'status': status,
#             'furnishing': furnishing,
#             'floor': floor,
#             'facing': facing,
#             'overlook': overlooking
#         }

#     def parse(self, response):
#         # Extract flat information
#         flats = response.css('div.mb-srp__list')
#         for flat in flats:
#             flat_info = self.parse_flat(flat)
#             self.items.append(flat_info)
#             # Extract JSON script data
#             script_tags = flat.xpath('.//script[@type="application/ld+json"]/text()').getall()
#             for script_text in script_tags:
#                 try:
#                     data = json.loads(script_text)
#                     url_value = data.get('url')
#                     geo_location = data.get('geo')
#                     address = data.get('address')
#                     if geo_location:
#                         latitude=geo_location.get('latitude')
#                         longitude=geo_location.get('longitude')
#                     if address:
#                         addressLocality=address.get('addressLocality')
#                         addressRegion=address.get('addressRegion')
                    
#                     item = {
#                         **flat_info,
#                         'url': url_value,
#                         'latitude':latitude,
#                         'longitude':longitude,
#                         'addressLocality': addressLocality,
#                         'addressRegion': addressRegion
#                     }
#                     self.houses['data'].append(item)
#                     self.items.append(item)
#                 except json.JSONDecodeError:
#                     self.logger.error("Error decoding JSON in script tag")

#         # Sending request to next url of each flat for extracting next url information
#         for item in self.houses['data']:
#             url = item.get('url')
#             if url:
#                 yield scrapy.Request(url=url, callback=self.parse_url, meta={'flat_info': item})
#             else:
#                 self.logger.warning("Skipping invalid URL: %s", url)
        
#         #Crawling to next page url
#         # if self.page_count < 30:
#         #     self.page_count += 1
#         #     next_page_relative_url = response.css('li.mb-pagination__list--item.active + li.mb-pagination__list--item a::attr(href)').get()
#         #     if next_page_relative_url:
#         #         next_page_url = response.urljoin(next_page_relative_url)
#         #         yield scrapy.Request(url=next_page_url, callback=self.parse)

#     def parse_url(self, response):
#         #Create self.house 
#         flat_info = response.meta.get('flat_info', {})

#         # Extract number of beds
#         beds = response.css('[data-icon="beds"] span.mb-ldp__dtls__body__summary--highlight::text').get()
#         flat_info['Beds'] = beds.strip() if beds else '1'

#         # Extract number of bathrooms
#         bathroom = response.css('[data-icon="baths"] span.mb-ldp__dtls__body__summary--highlight::text').get()
#         flat_info['bathroom'] = bathroom.strip() if bathroom else '1'
      
#         # Extract number of balconies
#         balcony = response.css('[data-icon="balcony"] span.mb-ldp__dtls__body__summary--highlight::text').get()
#         balconies = response.css('[data-icon="balconies"] span.mb-ldp__dtls__body__summary--highlight::text').get()
#         flat_info['balcony'] = balconies if balconies is not None else balcony

#         # Extract number of covered parking
#         parking = response.css('[data-icon="covered-parking"] span.mb-ldp__dtls__body__summary--highlight::text').get()
#         flat_info['parking'] = parking if parking is not None else parking

#         # Extracting amenities
#         amenities = []
#         amenity = response.css('div.mb-ldp__amenities li::text').getall()
#         extra_amenity = response.css('.mb-ldp__dtls__body__summary--right__icons .mb-ldp__dtls__body__summary--item::text').getall()
#         amenities.extend(amenity)
#         amenities.extend(extra_amenity)
#         flat_info['amenities'] = amenities
        
#         self.items.append(flat_info)


#     def closed(self, reason):
#        if self.houses['data']:
#           fieldnames = self.houses['data'][0].keys()
#           output_file = 'out.csv'
#           with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#             writer.writeheader()
#             writer.writerows(self.houses['data'])
#        else:
#             self.logger.warning("No data found to write to CSV.")



    # def csv_file(self):
    #    fieldnames = self.houses['data'][0].keys()
    #    with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
    #    writer = csv.DictWriter(self.csvfile(), fieldnames=fieldnames)
    #    writer.writeheader()
    #    for item in self.houses['data']:
    #        writer.writerow(item)



    # price='$100,000',
    # area_sqft='1000 sqft',
    # property_name='Sample Property',
    # image_url='https://example.com/sample-image.jpg',
    # carpet_area='800 sqft',
    # super_area='1200 sqft',
    # status='For Sale',
    # furnishing='Furnished',
    # facing='North',
    # floor='5th Floor',
    # overlook='Park'
    # Add other fields as necessary
    # )       



import scrapy
from housescraper.items import HouseItem
import json
import re
import csv

class MySpider(scrapy.Spider):
    name = 'housespider'
    allowed_domains = ['magicbricks.com']
    start_urls = ['https://www.magicbricks.com/flats-in-mumbai-for-sale-pppfs']

    def __init__(self):
        super(MySpider, self).__init__()
        self.items=[]
        self.page_count = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_flat(self, flat):
        item = HouseItem()
        item['title'] = flat.css('h2.mb-srp__card--title::text').get()
        item['price'] = flat.css('div.mb-srp__card__price--amount::text').get()
        item['area_sqft'] = flat.css('div.mb-srp__card__price--size::text').get()
        item['property_name'] = flat.css('a.mb-srp__card__society--name::text').get()
        item['image_url'] = flat.css('div.mb-srp__card__photo__fig img::attr(data-src)').get()

        item['carpet_area'] = flat.css('[data-summary="carpet-area"] .mb-srp__card__summary--value::text').get()
        item['super_area'] = flat.css('[data-summary="super-area"] .mb-srp__card__summary--value::text').get()
        item['status'] = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Status")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        item['furnishing'] = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Furnishing")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        item['facing'] = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "facing")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        item['floor'] = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "Floor")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        item['overlook'] = flat.xpath('.//div[@class="mb-srp__card__summary--label" and contains(text(), "overlooking")]/following-sibling::div[@class="mb-srp__card__summary--value"]/text()').get()
        
        return item

    def parse(self, response):
        # Extract flat information
        for flat in response.css('div.mb-srp__list'):
            flat_item = self.parse_flat(flat)

            # Extract JSON script data
            script_tags = flat.xpath('.//script[@type="application/ld+json"]/text()').getall()
            for script_text in script_tags:
                try:
                    data = json.loads(script_text)
                    url_value = data.get('url')
                    geo_location = data.get('geo')
                    address = data.get('address')
                    if geo_location:
                        flat_item['latitude'] = geo_location.get('latitude')
                        flat_item['longitude'] = geo_location.get('longitude')
                    if address:
                        flat_item['addressLocality'] = address.get('addressLocality')
                        flat_item['addressRegion'] = address.get('addressRegion')
                    
                    flat_item['url'] = url_value
                    url=flat_item['url']
                    if url:
                      yield scrapy.Request(url=url, callback=self.parse_url, meta={'flat_info': flat_item})
                    else:
                        self.logger.warning("Skipping invalid URL: %s", url)
                except json.JSONDecodeError:
                    self.logger.error("Error decoding JSON in script tag")

            yield flat_item

        # Crawling to next page url
        # if self.page_count < 5:
        #     self.page_count += 1
        #     next_page_relative_url = response.css('li.mb-pagination__list--item.active + li.mb-pagination__list--item a::attr(href)').get()
        #     if next_page_relative_url:
        #         next_page_url = response.urljoin(next_page_relative_url)
        #         yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_url(self, response):
        flat_item = response.meta['flat_info']
        propertyid_element=response.css('.mb-ldp__posted--propid').get()
        flat_item['property_id']=re.search(r'\d+',propertyid_element).group()
        flat_item['Landmark']=response.css('a.mb-ldp__dtls__title--link::text').get()
        flat_item['Beds'] = response.css('[data-icon="beds"] span.mb-ldp__dtls__body__summary--highlight::text').get().strip() if response.css('[data-icon="beds"] span.mb-ldp__dtls__body__summary--highlight::text').get() else '1'
        flat_item['bathroom'] = response.css('[data-icon="baths"] span.mb-ldp__dtls__body__summary--highlight::text').get().strip() if response.css('[data-icon="baths"] span.mb-ldp__dtls__body__summary--highlight::text').get() else '1'
        balcony = response.css('[data-icon="balconies"] span.mb-ldp__dtls__body__summary--highlight::text').get()
        if not balcony:
            balcony = response.css('[data-icon="balcony"] span.mb-ldp__dtls__body__summary--highlight::text').get()
        flat_item['balcony'] = balcony.strip() if balcony else '0'
        flat_item['parking'] = response.css('[data-icon="covered-parking"] span.mb-ldp__dtls__body__summary--highlight::text').get().strip() if response.css('[data-icon="covered-parking"] span.mb-ldp__dtls__body__summary--highlight::text').get() else '0'
        flat_item['amenities'] = response.css('div.mb-ldp__amenities li::text').getall() + response.css('.mb-ldp__dtls__body__summary--right__icons .mb-ldp__dtls__body__summary--item::text').getall()
        yield flat_item
        url_overview=response.css('a.mb-ldp__dtls__title--link::attr(href)').get()
        if url_overview:
           yield scrapy.Request(url=url_overview, callback=self.parse_overview, meta={'flat_info': flat_item})
        else:
            self.logger.warning("Skipping invalid URL: %s", url_overview)

    #Extracting overview data from the next overview url of each Landmark
    def parse_overview(self, response):
        flat_item = response.meta['flat_info']
        flat_item['NearbyLocality'] = response.css('div.factoids__card__body__item::text').getall()
        rating = []
        review_title= response.css('div.loc-det__livablityblock__reviewtitle::text').getall()
        review_value=response.css('div.loc-det__livablityblock__reviewvalue::text').getall()
        
        for title,value in zip(review_title, review_value):
            review_combined=f'{title} {value}'
            rating.append(review_combined)
        
        landmark_title=response.css('h1::text').get().strip()
        review_value=response.css('.loc-det__blocks__ratinglabel::text').get()
        review_landmark= f'{landmark_title} {review_value}/5'
        rating.append(review_landmark)

        # for landmark,review in zip(landmark_title,review_value):
        #     review_landmark= f'{landmark_title} {review}'
        #     rating.append(review_landmark)

        flat_item['rating']=rating


        yield flat_item



# response.css('.mb-ldp__more-dtl__list').get()









#More details from Landmark Overview
# flat_item['NearbyLocality']=response.css('div.factoids__card__body__item::text').getall()

# Rating data Extracted
# reviews_combined = []

# Iterate over review titles and values simultaneously
# review_title= response.css('div.loc-det__livablityblock__reviewtitle::text').get()
# review_value=response.css('div.loc-det__livablityblock__reviewvalue::text').get()
# for title, value in zip(review_title, review_value):
#     review_combined = f"{title} {value}"  # Concatenate title and value
#     reviews_combined.append(review_combined)

# print(reviews_combined)
#Rating=[Environment(rating), ]

# landmark_title=response.css('h1::text').get().strip()
# review_value=response.css('.loc-det__blocks__ratinglabel::text').get()
import scrapy
from housescraper.items import HouseItem
import json
import re
import numpy as np
import csv
import requests
from bs4 import BeautifulSoup
from scrapy.http import Request

class MySpider(scrapy.Spider):
    name = 'housespider'
    allowed_domains = ['magicbricks.com']
    # start_urls = ['https://www.magicbricks.com/flats-in-mumbai-for-sale-pppfs']
    start_urls=['https://www.magicbricks.com/flats-in-new-delhi-for-sale-pppfs']

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

        carpet_area = flat.css('[data-summary="carpet-area"] .mb-srp__card__summary--value::text').get()
        super_area = flat.css('[data-summary="super-area"] .mb-srp__card__summary--value::text').get()

        if carpet_area:
           item['area'] = f'Carpet area:{carpet_area}'
        elif super_area:
           item['area'] = f'Super area:{super_area}'
        else:
           item['area'] = 'Nan'
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
        if self.page_count < 2:
            self.page_count += 1
            next_page_relative_url = response.css('li.mb-pagination__list--item.active + li.mb-pagination__list--item a::attr(href)').get()
            if next_page_relative_url:
                next_page_url = response.urljoin(next_page_relative_url)
                yield scrapy.Request(url=next_page_url, callback=self.parse)

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
        flat_item['url_overview']=response.css('a.mb-ldp__dtls__title--link::attr(href)').get()
        

        #Scraping with Beautiful Soup the more details of the flats
        soup = BeautifulSoup(response.text, 'html.parser') 
        property_det=[]

        div_label = soup.find_all('div', class_='mb-ldp__more-dtl__list--label')
        div_value = soup.find_all('div', class_='mb-ldp__more-dtl__list--value')
        
        for label, value in zip(div_label, div_value):
            url_data=[]
            label_text = label.text.strip()
            value_text = value.text.strip()            
            # if label_text in ['Flooring', 'Age of Construction', 'Water Availability', 'Status of Electricity']:
            url_data.append(f'{label_text}: {value_text}')
            property_det.append(url_data)
        flat_item['flat_details']=property_det
        
        # Request the URL overview page
        yield Request(flat_item['url_overview'], callback=self.parse_url_overview, meta={'flat_item': flat_item})
     

    #Scraping Data with BeautifulSoup
    def parse_url_overview(self, response):
        # Retrieve flat_item from meta
        flat_item = response.meta['flat_item']
        
        # Use Beautiful Soup to parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Initialize an empty list for h1
        Locality = []

        # Find all <div> elements with class 'factoids__card__body__item'
        div_elements = soup.find_all('div', class_='factoids__card__body__item')
        
        # Iterate over each <div> element
        for div_element in div_elements:
            # Find the text within the <div> element and append it to h1 list
            Locality.append(div_element.text.strip())

        # Assign h1 list to 'NearbyLocality' in flat_item
        flat_item['NearbyLocality'] = Locality
        
        # Yield the modified flat_item
        yield flat_item







    #     if flat_item['url_overview']:
    #        yield scrapy.Request(url=flat_item['url_overview'], callback=self.parse_overview, meta={'flat_info': flat_item})
    #     else:
    #         self.logger.warning("Skipping invalid URL: %s", flat_item['url_overview'])

    # #Extracting overview data from the next overview url of each Landmark
    # def parse_overview(self, response):
    #     flat_item = response.meta['flat_info']
    #     flat_item['NearbyLocality'] = response.css('div.factoids__card__body__item::text').getall().strip()
    #     rating = []
    #     review_title= response.css('div.loc-det__livablityblock__reviewtitle::text').getall()
    #     review_value=response.css('div.loc-det__livablityblock__reviewvalue::text').getall()
        
    #     for title,value in zip(review_title, review_value):
    #         review_combined=f'{title} {value}'
    #         rating.append(review_combined)
        
    #     landmark_title=response.css('h1::text').get().strip()
    #     review_value=response.css('.loc-det__blocks__ratinglabel::text').get()
    #     review_landmark= f'{landmark_title} {review_value}/5'
    #     rating.append(review_landmark)

    #     flat_item['rating']=rating


    #     yield flat_item



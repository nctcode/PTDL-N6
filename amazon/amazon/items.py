# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonItem(scrapy.Item):
    productid = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    publication_date = scrapy.Field()
    old_price = scrapy.Field()
    new_price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()
    page_count = scrapy.Field()
    language = scrapy.Field()
    publisher_name = scrapy.Field()
    review_name = scrapy.Field()
    review_rating = scrapy.Field()
    review_comment = scrapy.Field()
    review_date = scrapy.Field()

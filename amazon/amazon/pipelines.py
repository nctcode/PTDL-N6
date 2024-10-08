# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy
import pymongo 
from pymongo import MongoClient
import json
from scrapy.exceptions import DropItem
import csv
import os

class CSVDBAmazonPipeline:
    def process_item(self, item, spider):
        with open('amazon.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='$')
            writer.writerow([
                item['productid'], 
                item['title'],
                item['author'],
                item['publication_date'],
                item['old_price'], 
                item['new_price'],
                item['rating'],
                item['reviews'], 
                item['page_count'],
                item['language'],
                item['publisher_name'], 
                item['review_name'],
                item['review_rating'],
                item['review_comment'], 
                item['review_date']
            ])
        return item
    pass

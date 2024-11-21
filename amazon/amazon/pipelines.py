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
class MongoDBAmazonPipeline:
    def __init__(self):
        # connection String
        self.client = pymongo.MongoClient('mongodb://localhost:27017')
        self.db = self.client['dbmycrawler'] # Create Database      
        pass
    
    def process_item(self, item, spider):
        
        collection =self.db['tblamazon1'] # Create Colecction or Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")       
        pass   

class JsonDBAmazonPipeline:
    def process_item(self, item, spider):
        with open('amazon1amazon1.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBAmazonPipeline:
    def process_item(selfamazon1.csv', 'a', encoding='utf-8', newline='') as file:
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

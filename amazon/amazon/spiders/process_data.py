import os
import pymongo
import pandas as pd
from sqlalchemy import create_engine

mongo_host = os.environ.get('Mongo_HOST', 'mymongodb')
mongo_port = os.environ.get('Mongo_PORT', '27017')

client = pymongo.MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')
db = client['dbmycrawler']
collection = db['tblamazon']
data = list(collection.find())
df = pd.DataFrame(data)

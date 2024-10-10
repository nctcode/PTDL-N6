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

# Xử lý dữ liệu
df['title'] = df['title'].str.split(r'[:(]').str[0] # Chia tách tiêu đề để loại bỏ phần mô tả sau dấu ':' hoặc '('
df['publication_date'] = pd.to_datetime(df['publication_date'], format='%B %d, %Y', errors='coerce') # Chuyển đổi ngày xuất bản sang định dạng datetime
df['old_price'] = df['old_price'].str.replace('$', '', regex=False).astype(float) # Loại bỏ ký tự '$' khỏi giá mới nếu giá đó là chuỗi
df['new_price'] = df['new_price'].str.replace('$', '', regex=False).astype(float) 



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

df['page_count'] = df['page_count'].str.replace(' pages', '', regex=True)  # Loại bỏ chuỗi ' pages' khỏi số lượng trang
df['reviews'] = df['reviews'].str.replace(',', '', regex=True)  # Loại bỏ dấu ',' trong số lượng đánh giá
df['reviews'] = df['reviews'].astype(int)  # Chuyển đổi reviews thành kiểu int
df = df.drop_duplicates(subset=['productid'], keep='first')  # Loại bỏ các dòng trùng lặp dựa trên productid

df['publisher_name'] = df['publisher_name'].str.split(r'[:;()]').str[0]
df['review_rating'] = df['review_rating'].str.split('out').str[0]
df['review_date'] = df['review_date'].str.split('on').str[1].str.strip()
df['review_date'] = pd.to_datetime(df['review_date'], format='%B %d, %Y', errors='coerce')
df.replace("", np.nan, inplace=True)
df.replace(" ", np.nan, inplace=True)
df.dropna()

postgres_user = 'postgres'  # Tên người dùng PostgreSQL, thường là 'postgres'
postgres_password = '12345'  # Mật khẩu của bạn cho PostgreSQL
postgres_host = 'localhost'  # Địa chỉ máy chủ PostgreSQL, thường là 'localhost' nếu chạy trên máy cá nhân
postgres_port = '5432'  # Cổng mặc định của PostgreSQL là 5432
postgres_db = 'AmazonBook'  # Tên cơ sở dữ liệu PostgreSQL mà bạn muốn kết nối

# Tạo chuỗi kết nối đến cơ sở dữ liệu PostgreSQL
engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}')

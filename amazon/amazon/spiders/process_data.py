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

publishers_df = df[['publisher_name']].drop_duplicates()  # Tạo DataFrame chỉ chứa tên nhà xuất bản và loại bỏ các dòng trùng lặp
publishers_df.to_sql('publishers', engine, if_exists='append', index=False)  # Đưa dữ liệu vào bảng publishers trong cơ sở dữ liệu

publishers_with_id_df = pd.read_sql('SELECT publisher_id, publisher_name FROM publishers', engine)  # Chọn bảng products
df = df.merge(publishers_with_id_df, on='publisher_name', how='left')  # Kết hợp với publisher_id
publishers_df = pd.read_sql('SELECT publisher_id, publisher_name FROM publishers', engine)  # Lấy dữ liệu từ bảng publishers
merged_df = df.merge(publishers_df, left_on='publisher_id', right_on='publisher_id', how='left')  # Gộp để thêm publisher_id
products_df = merged_df[['productid', 'title', 'author', 'publication_date', 'old_price', 'new_price', 'rating', 'reviews', 'page_count', 'language', 'publisher_id']]  # Chọn các cột cần thiết để tạo DataFrame cho bảng products
products_df.to_sql('products', engine, if_exists='append', index=False)  # Đưa dữ liệu vào bảng products trong cơ sở dữ liệu

products_df = pd.read_sql('SELECT productid, title FROM products', engine)  # Lấy lại thông tin sản phẩm từ cơ sở dữ liệu bao gồm productid và title
merged_df = df.merge(products_df, left_on='productid', right_on='productid', how='left')  # Gộp DataFrame ban đầu với DataFrame sản phẩm để lấy thông tin đánh giá
reviews_df = merged_df[['productid', 'review_name', 'review_rating', 'review_comment', 'review_date']]  # Chọn các cột cần thiết để tạo DataFrame cho bảng reviews
reviews_df.to_sql('reviews', engine, if_exists='append', index=False)  # Đưa dữ liệu vào bảng reviews trong cơ sở dữ liệu
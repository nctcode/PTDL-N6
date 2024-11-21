import os
import pymongo
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2

# MongoDB connection
mongo_host = os.environ.get('Mongo_HOST', 'mymongodb')
mongo_port = os.environ.get('Mongo_PORT', '27017')

client = pymongo.MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')
db = client['dbmycrawler']
collection = db['tblamazon1']
data = list(collection.find())

# Chuyển dữ liệu MongoDB thành DataFrame
df = pd.DataFrame(data)

# Đặt tên cho các cột của DataFrame
column_names = [
    'id', 'category', 'product_id', 'title', 'author', 'publication_date', 
    'old_price', 'new_price', 'rating', 'reviews', 
    'page_count', 'language', 'publisher_name', 'discount', 
    'reviewer_name', 'reviewer_rating', 'review_comment',
    'review_date',
    'five_star_percent', 'four_star_percent', 'three_star_percent', 'two_star_percent', 'one_star_percent',
    'best_sellers_rank'
]

df.columns = column_names

# 1. Xóa các dòng có "review_name" là rỗng
df = df[df["reviewer_name"].notnull() & (df["reviewer_name"] != "")]

# Xóa cột 'id'
df = df.drop(columns=['id'])

# Xóa các dòng có giá trị NaN và loại bỏ bản ghi trùng lặp
df = df.dropna()
df.drop_duplicates(inplace=True)

# Bước 1: Thêm cột 'product_id' với giá trị tuần tự
df = df.drop(columns=['product_id'])
df['product_id'] = range(1, len(df) + 1)

# Bước 2: Xử lý cột 'old_price' và 'new_price'
df[['old_price', 'new_price']] = df[['old_price', 'new_price']].replace({'\$': ''}, regex=True)
df['old_price'] = pd.to_numeric(df['old_price'], errors='coerce')
df['new_price'] = pd.to_numeric(df['new_price'], errors='coerce')

# Bước 3: Xử lý cột 'discount'
df['discount'] = df['discount'].str.replace(r'[()]', '', regex=True).str.strip().str.rstrip('%').astype(float) / 100

# Bước 4: Chuyển đổi các phần trăm sao
percent_columns = ['five_star_percent', 'four_star_percent', 'three_star_percent', 'two_star_percent', 'one_star_percent']
df[percent_columns] = df[percent_columns].replace('%', '', regex=True).astype(float) / 100

# Bước 5: Làm sạch và rút gọn cột 'title'
df['title'] = df['title'].str.split(r'[:(]').str[0]

# Bước 6: Chuyển đổi cột 'publication_date' thành datetime
df['publication_date'] = pd.to_datetime(df['publication_date'], format='%B %d, %Y', errors='coerce')

# Bước 7: Xử lý cột 'page_count'
df = df[df['page_count'].str.strip() != ""]
df['page_count'] = df['page_count'].str.replace('pages', '', regex=False).astype(int)

# Bước 8: Xử lý cột 'reviews'
# Loại bỏ các chuỗi 'ratings' hoặc 'rating' và thay đổi dấu ',' thành ''
df['reviews'] = df['reviews'].str.replace('ratings', '', regex=False)
df['reviews'] = df['reviews'].str.replace(',', '', regex=False).str.strip()
df = df[df['reviews'].str.isdigit()]
df['reviews'] = df['reviews'].astype(int)

# Bước 9: Xử lý cột 'language'
valid_languages = ['English', 'Spanish', 'French', 'German', 'Chinese']
df['language'] = df['language'].where(df['language'].isin(valid_languages), 'English')

# Bước 10: Làm sạch cột 'publisher_name'
df['publisher_name'] = df['publisher_name'].str.split(r'[:;()]').str[0]

# Bước 11: Loại bỏ các dòng có 'publisher_name' bắt đầu bằng tên tháng
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 
          'August', 'September', 'October', 'November', 'December']
df = df[~df['publisher_name'].str.startswith(tuple(months))]

# Bước 12: Xóa các dòng rỗng/null trong cột 'publisher_name'
df = df[df['publisher_name'].notna() & (df['publisher_name'] != '')]

# Bước 13: Chuyển đổi cột 'reviewer_rating'
df['reviewer_rating'] = pd.to_numeric(df['reviewer_rating'].str.split('out').str[0], errors='coerce')

# Bước 14: Xử lý cột 'best_sellers_rank'
df['best_sellers_rank'] = df['best_sellers_rank'].str.replace('#', '', regex=False).str.replace(',', '', regex=False).astype(int)

# Bước 15: Tách cột 'review_date' thành 'reviewer_address' và 'review_date'
df[['reviewer_address', 'review_date']] = df['review_date'].str.split(' on ', expand=True)

# Bước 16: Làm sạch 'reviewer_address'
df['reviewer_address'] = df['reviewer_address'].str.replace('Reviewed in the ', '', regex=False).str.replace('Reviewed in', '', regex=False).str.strip()

# Bước 17: Chuyển đổi 'review_date' thành datetime
df['review_date'] = pd.to_datetime(df['review_date'], format='%B %d, %Y', errors='coerce')

df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

# Bước 18: Tính 'sold_quantity'
df["sold_quantity"] = (df["reviews"] * df["rating"]).astype(int)

# Bước 19: Sắp xếp lại thứ tự các cột
new_columns = [
    'product_id', 'category', 'title', 'author', 'publisher_name', 'publication_date', 
    'old_price', 'new_price', 'discount', 'page_count', 'language', 'best_sellers_rank', 'reviews', 'rating', 
    'sold_quantity', 'five_star_percent', 'four_star_percent', 'three_star_percent', 'two_star_percent', 
    'one_star_percent', 'reviewer_name', 'reviewer_address', 'reviewer_rating', 'review_comment', 'review_date',
]
df = df.reindex(columns=new_columns)

# 20. Đổi tên các cột để phù hợp với tên cột trong PostgreSQL
df = df.rename(columns={
    "category": "category_name",
    "publisher_name": "publisher_name"
})

# Kết nối tới PostgreSQL
conn = psycopg2.connect(
    dbname="AmazonBook2", 
    user="postgres", 
    password="Thuan2903", 
    host="postgres_app", 
    port="5432"
)
cursor = conn.cursor()

# Tạo bảng Categorys
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Categorys (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(255) UNIQUE NOT NULL
    );
""")

# Tạo bảng Publishers
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Publishers (
        publisher_id SERIAL PRIMARY KEY,
        publisher_name VARCHAR(255) UNIQUE NOT NULL
    );
""")

# Tạo bảng Products
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        product_id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        author VARCHAR(255),
        category_id INT REFERENCES Categorys(category_id),
        publisher_id INT REFERENCES Publishers(publisher_id),
        publication_date DATE,
        old_price NUMERIC(10, 2),
        new_price NUMERIC(10, 2),
        discount NUMERIC(5, 2),
        page_count INT,
        language VARCHAR(50),
        best_sellers_rank INT,
        reviews INT,
        rating NUMERIC(3, 2),
        sold_quantity INT
    );
""")

# Tạo bảng Ratings
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Ratings (
        rating_id SERIAL PRIMARY KEY,
        product_id INT REFERENCES Products(product_id),
        five_star_percent NUMERIC(5, 2),
        four_star_percent NUMERIC(5, 2),
        three_star_percent NUMERIC(5, 2),
        two_star_percent NUMERIC(5, 2),
        one_star_percent NUMERIC(5, 2)
    );
""")

# Tạo bảng Reviewers
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Reviewers (
        reviewer_id SERIAL PRIMARY KEY,
        reviewer_name TEXT,
        reviewer_address TEXT,
        reviewer_rating INT,
        review_comment TEXT,
        review_date DATE,
        product_id INT REFERENCES Products(product_id)
    );
""")
conn.commit()


# Bước 1: Lấy các thể loại duy nhất và kiểm tra số lượng
category_names = df['category_name'].unique().tolist()

# Kiểm tra số lượng thể loại, nếu có 6 thể loại thì chèn vào bảng Categorys
if len(category_names) == 6:
    # Kết nối tới PostgreSQL
    conn = psycopg2.connect(
        dbname="AmazonBook2", 
        user="postgres", 
        password="Thuan2903", 
        host="postgres_app", 
        port="5432"
    )
    cursor = conn.cursor()

    # Chèn các thể loại vào bảng Categorys nếu chưa có
    for index, category_name in enumerate(category_names, start=1):
        try:
            cursor.execute("""
                INSERT INTO Categorys (category_id, category_name) 
                VALUES (%s, %s)
                ON CONFLICT (category_name) DO NOTHING;
            """, (index, category_name))
        except Exception as e:
            pass  # Bỏ qua lỗi nếu có

    # Commit thay đổi vào database
    conn.commit()

    # Bước 2: Chèn dữ liệu vào bảng Publishers và Products
    for index, row in df.iterrows():
        try:
            # Kiểm tra và thêm nhà xuất bản vào bảng Publishers nếu chưa có
            cursor.execute(""" 
                SELECT publisher_id FROM Publishers WHERE publisher_name = %s;
            """, (row['publisher_name'],))
            publisher_id_result = cursor.fetchone()
            
            if not publisher_id_result:  # Nếu không tìm thấy publisher_name
                # Thêm nhà xuất bản mới vào bảng Publishers và lấy publisher_id
                cursor.execute("""
                    INSERT INTO Publishers (publisher_name) 
                    VALUES (%s) 
                    RETURNING publisher_id;
                """, (row['publisher_name'],))
                publisher_id = cursor.fetchone()[0]
            else:
                publisher_id = publisher_id_result[0]  # Nếu đã có nhà xuất bản, lấy publisher_id
            
            # Commit thay đổi vào database sau khi thêm nhà xuất bản
            conn.commit()
            
            # Kiểm tra và lấy category_id
            cursor.execute("""
                SELECT category_id FROM Categorys WHERE category_name = %s;
            """, (row['category_name'],))
            category_id_result = cursor.fetchone()
            
            if category_id_result:
                category_id = category_id_result[0]
            else:
                continue  # Nếu không tìm thấy thể loại thì bỏ qua

            # Chèn dữ liệu vào bảng Products
            cursor.execute("""
                INSERT INTO Products (product_id, title, author, category_id, publisher_id, 
                publication_date, old_price, new_price, discount, page_count, language, 
                best_sellers_rank, reviews, rating, sold_quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (row['product_id'], row['title'], row['author'], category_id, publisher_id, 
                  row['publication_date'], row['old_price'], row['new_price'], row['discount'], 
                  row['page_count'], row['language'], row['best_sellers_rank'], row['reviews'], 
                  row['rating'], row['sold_quantity']))

            # Chèn dữ liệu vào bảng Ratings
            cursor.execute("""
                INSERT INTO Ratings (product_id, five_star_percent, four_star_percent, 
                three_star_percent, two_star_percent, one_star_percent)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (row['product_id'], row['five_star_percent'], row['four_star_percent'], 
                  row['three_star_percent'], row['two_star_percent'], row['one_star_percent']))

            # Chèn dữ liệu vào bảng Reviewers
            cursor.execute("""
                INSERT INTO Reviewers (reviewer_name, reviewer_address, reviewer_rating, 
                review_comment, review_date, product_id)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (row['reviewer_name'], row['reviewer_address'], row['reviewer_rating'], 
                  row['review_comment'], row['review_date'], row['product_id']))
            
        except Exception as e:
            print(f"Error occurred: {e}")
            pass  

    # Commit các thay đổi vào PostgreSQL
    conn.commit()

    # Đóng kết nối
    cursor.close()
    conn.close()

    print("Dữ liệu đã được lưu vào PostgreSQL thành công!")
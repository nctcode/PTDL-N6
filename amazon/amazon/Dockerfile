FROM python:3.10-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép tệp yêu cầu vào container
COPY requirements.txt .

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn vào container
COPY . .

# Chạy tệp Python chính của bạn
CMD ["python", "spiders/processData.py"]

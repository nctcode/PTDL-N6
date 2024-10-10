BOT_NAME = "amazon"

SPIDER_MODULES = ["amazon.spiders"]
NEWSPIDER_MODULE = "amazon.spiders"

ROBOTSTXT_OBEY = False

# Tăng số lượng yêu cầu đồng thời
CONCURRENT_REQUESTS = 50
CONCURRENT_REQUESTS_PER_DOMAIN = 50
CONCURRENT_REQUESTS_PER_IP = 16

# Giảm độ trễ giữa các yêu cầu
DOWNLOAD_DELAY = 1

# Bật AutoThrottle để tự động điều chỉnh tốc độ
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1  # Giảm độ trễ bắt đầu
AUTOTHROTTLE_MAX_DELAY = 3  # Tăng cường độ tối đa
AUTOTHROTTLE_TARGET_CONCURRENCY = 5.0  # Tăng tốc độ yêu cầu

# Điều chỉnh header để trông giống yêu cầu từ trình duyệt thật
DEFAULT_REQUEST_HEADERS = {
    'Accept-Language': 'en-US,en;q=0.9',
    'DNT': '1',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.amazon.com/',
}

# Tăng số lần retry nhưng ở mức hợp lý để không làm chậm quá trình cào dữ liệu
RETRY_TIMES = 10
RETRY_DELAY = 3  # Điều chỉnh thời gian delay giữa các lần retry

# Cài đặt thời gian chờ tải xuống hợp lý để tránh việc tải trang quá lâu
DOWNLOAD_TIMEOUT = 10

# Cấu hình middlewares để sử dụng Random User-Agent
DOWNLOADER_MIDDLEWARES = {
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
}

# Cấu hình pipelines để lưu dữ liệu vào MongoDB, JSON và CSV
ITEM_PIPELINES = {
    "amazon.pipelines.CSVDBAmazonPipeline": 100,
    "amazon.pipelines.JsonDBAmazonPipeline": 200,
    "amazon.pipelines.MongoDBAmazonPipeline": 400
}

# Vô hiệu hóa HTTP cache
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400

# Cấu hình phiên bản của request fingerprinter và reactor
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Đặt giới hạn số lượng item cào được trước khi đóng spider
CLOSESPIDER_ITEMCOUNT = 1800
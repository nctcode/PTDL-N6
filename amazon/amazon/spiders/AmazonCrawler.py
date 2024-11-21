import scrapy
from amazon.items import AmazonItem

class AmazonSpider(scrapy.Spider):
    name = "amazon"
    allowed_domains = ["amazon.com"]

   
    categories = {
        "History": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A156576011&dc&fs=true",
        "Law": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A156915011&dc&fs=true",
        "Nonfiction": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A157325011&dc&fs=true",
        "Romance": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A158566011&dc&fs=true",
        "Self-Help": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A156563011&dc&fs=true",
        "Travel": "https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A159936011&dc&fs=true",
    }

    def start_requests(self):
        for category_name, category_url in self.categories.items():
            yield scrapy.Request(
                url=category_url,
                callback=self.parse,
                meta={'category': category_name},  # Truyền tên thể loại vào meta
            )

    def parse(self, response):
        # Lấy thông tin thể loại từ meta
        category = response.meta['category']
        
        products = response.xpath('//div[contains(@class, "s-main-slot")]/div')
        for product in products:
            # Truy cập vào trang chi tiết sản phẩm
            product_url = product.xpath('.//h2/a/@href').get()
            if product_url:
                yield response.follow(
                    url=product_url,
                    callback=self.parse_product_details,
                    meta={'category': category}
                )

        # Phân trang
        next_page = response.xpath("//a[contains(@class, 's-pagination-next')]/@href").get()
        if next_page:
            yield response.follow(
                url=next_page,
                callback=self.parse,
                meta={'category': category}
            )

    def parse_product_details(self, response):
        item = AmazonItem()
        
        # Lấy thông tin thể loại từ meta
        item['category'] = response.meta['category']

        # Thông tin sách
        item['productid'] = response.xpath('//span[contains(text(),"ASIN")]/following-sibling::span/text()').get(default='').strip()
        item['title'] = response.xpath('//span[@id="productTitle"]/text()').get(default='').strip()
        item['author'] = response.xpath('//span[@class="author notFaded"]//a/text()').get(default='').strip()
        item['publication_date'] = response.xpath('//span[contains(text(),"Publication date")]/following-sibling::span/text()').get(default='')
        old_price = response.xpath('//*[@id="basis-price"]/text()').get(default='').strip()
        item['old_price'] = old_price if old_price else "$0.00"
        item['new_price'] = response.xpath('//span[@id="kindle-price"]/text()').get(default='').strip()
        item['rating'] = response.xpath('//*[@id="acrPopover"]/span/a/span/text()').get(default='').strip()
        item['reviews'] = response.xpath('//span[@id="acrCustomerReviewText"]/text()').get(default='').strip()
        item['page_count'] = response.xpath('//span[contains(text(),"Print length")]/following-sibling::span/text()').get(default='')
        item['language'] = response.xpath('//span[contains(text(),"Language")]/following-sibling::span/text()').get(default='')
        item['publisher_name'] = response.xpath('//*[@id="detailBullets_feature_div"]/ul/li[2]/span/span[2]/text()').get(default='').strip()
        item['discount'] = response.xpath('//*[@id="kindle-price-column"]/p/span[3]/text()').get(default='').strip()
        if not item['discount']:
            item['discount'] = '0%'

        # Thông tin khách hàng
        item['review_name'] = response.xpath('//div/div[5]/div/div/div[1]/div/div[2]/span/text()').get(default='').strip()
        item['review_rating'] = response.xpath('//div[5]/div/div/div[2]/i/span/text()').get(default='').strip()
        item['review_comment'] = response.xpath('//div[5]/div/div/div[2]/span[2]/span/text()').get(default='').strip()
        item['review_date'] = response.xpath('//span[2]/span/div[2]/div/div/div[5]/div/div/span/text()').get(default='').strip()

        # Phần trăm từng sao
        item['five_star_percent'] = response.xpath('//div[@class="a-section a-spacing-none a-text-right aok-nowrap"]/span[1]/text()').get(default='0').strip()
        item['four_star_percent'] = response.xpath('//div[@class="a-section a-spacing-none a-text-right aok-nowrap"]/span[2]/text()').get(default='0').strip()
        item['three_star_percent'] = response.xpath('//div[@class="a-section a-spacing-none a-text-right aok-nowrap"]/span[3]/text()').get(default='0').strip()
        item['two_star_percent'] = response.xpath('//div[@class="a-section a-spacing-none a-text-right aok-nowrap"]/span[4]/text()').get(default='0').strip()
        item['one_star_percent'] = response.xpath('//div[@class="a-section a-spacing-none a-text-right aok-nowrap"]/span[5]/text()').get(default='0').strip()

        best_sellers_rank = response.xpath('//span[contains(@class, "a-list-item")]/span[contains(text(), "Best Sellers Rank")]/following-sibling::text()').get()
        item['best_sellers_rank'] = best_sellers_rank.strip().split()[0] if best_sellers_rank else "0%"

        yield item

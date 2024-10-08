import scrapy
from amazon.items import AmazonItem

class AmazonSpider(scrapy.Spider):
    name = "amazon"
    allowed_domains = ["amazon.com"]
    start_urls = [
        'https://www.amazon.com/s?i=digital-text&rh=n%3A133140011%2Cn%3A154606011&dc=&fs=true&ds=v1%3AW6PF3616WWjSq3XME0%2B9b99iC9%2Fg70umHK8hKjuqoD0&qid=1727590145&rnid=133140011'
    ]

    def parse(self, response):
        products = response.xpath('//div[contains(@class, "s-result-item") and @data-asin]')
        if not products:
            self.logger.info('No products found on the page.')
            return

        for product in products:
            product_url = product.xpath('.//h2/a/@href').get()
            if product_url:
                yield response.follow(url=product_url, callback=self.parse_product_details)

        next_page = response.xpath("//a[contains(@class, 's-pagination-next')]/@href").get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse)

    def parse_product_details(self, response):
        item = AmazonItem()
        item['productid'] = response.xpath('//span[contains(text(),"ASIN")]/following-sibling::span/text()').get(default='').strip()
        item['title'] = response.xpath('//span[@id="productTitle"]/text()').get(default='').strip()
        item['author'] = response.xpath('//span[@class="author notFaded"]//a/text()').get(default='').strip()
        item['publication_date'] = response.xpath('//span[contains(text(),"Publication date")]/following-sibling::span/text()').get(default='')
        item['old_price'] = response.xpath('//*[@id="basis-price"]/text()').get(default='').strip()
        item['new_price'] = response.xpath('//span[@id="kindle-price"]/text()').get(default='').strip()
        item['rating'] = response.xpath('//*[@id="acrPopover"]/span/a/span/text()').get(default='').strip()
        item['reviews'] = response.xpath('//span[@id="acrCustomerReviewText"]/text()').get(default='').strip()
        item['page_count'] = response.xpath('//span[contains(text(),"Print length")]/following-sibling::span/text()').get(default='')
        item['language'] = response.xpath('//span[contains(text(),"Language")]/following-sibling::span/text()').get(default='')
        item['publisher_name'] = response.xpath('//*[@id="detailBullets_feature_div"]/ul/li[2]/span/span[2]/text()').get(default='').strip()
        item['review_name'] = response.xpath('//div[3]/div/div[1]/div/div/div[1]/a/div[2]/span/text()').get(default='').strip()
        item['review_rating'] = response.xpath('//div[1]/div/div/div[2]/a/i/span/text()').get(default='').strip()
        item['review_comment'] = response.xpath('//div/div[1]/div/div/div[2]/a/span[2]/text()').get(default='').strip()
        item['review_date'] = response.xpath('//div/div/div[3]/div[3]/div/div[1]/div/div/span/text()').get(default='').strip()

        yield item

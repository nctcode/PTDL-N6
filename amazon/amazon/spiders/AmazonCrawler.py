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

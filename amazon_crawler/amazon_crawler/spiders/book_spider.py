# -*- coding: utf-8 -*-
import scrapy
from amazon_crawler.items import Book


class BookSpiderSpider(scrapy.Spider):
    name = "book_spider"
    allowed_domains = ["www.amazon.cn"]
    start_urls = (
        'https://www.amazon.cn/%E5%9B%BE%E4%B9%A6/b/ref=sa_menu_top_books_l1?ie=UTF8&node=658390051',
    )
    paths = {
        'category_url': '//div[@class="categoryRefinementsSection"]/ul/li[not(@class)]/a/@href',
        'category_name': '//div[@class="categoryRefinementsSection"]/'
                         'ul/li[not(@class)]/a/span[@class="refinementLink"]/text()',
        'book_url': '//div[@id="mainResults" or @id="atfResults"]//div[@class="a-row a-spacing-small"]//a/@href',
        'book_list_page_next_link': '//a[@id="pagnNextLink"]/@href',
        'book_title': '//span[@id="btAsinTitle"]/span/text()|//span[@id="productTitle"]/text()'
    }

    def parse(self, response):
        category_urls = response.xpath(self.paths['category_url']).extract()
        category_names = response.xpath(self.paths['category_name']).extract()
        if 'path' not in response.meta:
            path = []
        else:
            path = response.meta['path']
        if category_urls:
            for i, url in enumerate(category_urls):
                yield scrapy.Request(response.urljoin(url), callback=self.parse,
                                     meta={'path': path + [category_names[i]]})
                if i == 1:
                    break
        else:
            book_urls = response.xpath(self.paths['book_url']).extract()
            for book_url in book_urls:
                yield scrapy.Request(book_url, callback=self.parse_books, meta={'path': path})
                # break
                # next_link = response.xpath(self.paths['book_list_page_next_link']).extract()
                # if next_link:
                #     yield scrapy.Request(response.urljoin(next_link[0]), callback=self.parse, meta={'path': path})

    def parse_books(self, response):
        book = Book()
        book['path'] = '/'.join(response.meta['path'])
        book['title'] = ''.join(response.xpath(self.paths['book_title']).re('[^\W]+'))
        book['url'] = response.url
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        yield book

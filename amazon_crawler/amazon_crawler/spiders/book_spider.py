# -*- coding: utf-8 -*-
import scrapy

from amazon_crawler.items import Book, Review, BookLoader, ReviewLoader


class BookSpiderSpider(scrapy.Spider):
    name = "book_spider"
    allowed_domains = ["www.amazon.cn"]
    start_urls = (
        'https://www.amazon.cn/%E5%9B%BE%E4%B9%A6/b/ref=sa_menu_top_books_l1?ie=UTF8&node=658390051',
    )
    paths = {
        'category_url': '//div[@class="categoryRefinementsSection"]/ul/li[not(@class)]/a/@href',
        'paths': '//div[@id="wayfinding-breadcrumbs_feature_div"]//span[@class="a-list-item"]/a/text()',
        'book_url': '//div[@id="mainResults" or @id="atfResults"]//div[@class="a-row a-spacing-small"]//a/@href',
        'book_list_page_next_link': '//a[@id="pagnNextLink"]/@href',
        'book_title': '//span[@id="btAsinTitle"]/span/text()|//span[@id="productTitle"]/text()',
        'review_link': '//a[@id="seeAllReviewsUrl"]/@href',
        'review_divs': '//div[@id="cm_cr-review_list"]//div[@class="a-section review"]',
        'review_next_page_link': '//div[@id="cm_cr-pagination_bar"]//li[@class="a-last"]/a/@href'
    }

    def parse(self, response):
        category_urls = response.xpath(self.paths['category_url']).extract()
        i = 0
        if category_urls:
            for url in category_urls:
                i += 1
                yield scrapy.Request(response.urljoin(url), callback=self.parse)
                if i == 1:
                    break
        else:
            book_urls = response.xpath(self.paths['book_url']).extract()
            for book_url in book_urls:
                yield scrapy.Request(book_url, callback=self.parse_books)
                next_link = response.xpath(self.paths['book_list_page_next_link']).extract()
                if next_link:
                    yield scrapy.Request(response.urljoin(next_link[0]), callback=self.parse)

    def parse_books(self, response):
        book_loader = BookLoader(item=Book(), response=response)
        # book_loader.add_xpath('path', self.paths['paths'])
        book_loader.add_xpath('title', self.paths['book_title'])
        book_loader.add_value('url', response.url)
        review_link = response.xpath(self.paths['review_link']).extract()
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        yield book_loader.load_item()
        for url in review_link:
            yield scrapy.Request(url, callback=self.parse_comments, meta={'book_loader': book_loader, 'reviews': []})

    def parse_comments(self, response):
        review_divs = response.xpath(self.paths['review_divs'])
        reviews = response.meta['reviews']
        for review_div in review_divs:
            review_loader = ReviewLoader(item=Review(), selector=review_div)
            review_loader.add_css('title', '.review-title')
            review_loader.add_css('author', '.author')
            review_loader.add_css('content', '.review-text')
            review_loader.add_css('date', '.review-date')
            reviews.append(dict(review_loader.load_item()))

        next_page_link = response.xpath(self.paths['review_next_page_link']).extract()
        if next_page_link and len(reviews) < 20:
            yield scrapy.Request(response.urljoin(next_page_link[0]), callback=self.parse_comments,
                                 meta={'reviews': reviews, 'book_loader': response.meta['book_loader']})
        else:
            book_loader = response.meta['book_loader']
            book_loader.add_value('reviews', reviews)
            yield book_loader.load_item()

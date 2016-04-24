# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Compose, Join, MapCompose
from w3lib.html import remove_tags


class Book(scrapy.Item):
    # define the fields for your item here like:
    # path = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    reviews = scrapy.Field()
    pass


class Review(scrapy.Item):
    title = scrapy.Field()
    author = scrapy.Field()
    content = scrapy.Field()
    date = scrapy.Field()


class BookLoader(ItemLoader):
    default_item_class = Book
    url_out = TakeFirst()
    title_out = Compose(TakeFirst(), lambda s: s.strip())
    # path_out = Compose(MapCompose(lambda s: s.strip()), Join('/'))
    pass


class ReviewLoader(ItemLoader):
    default_item_class = Review
    default_output_processor = Compose(TakeFirst(), remove_tags)
    date_out = Compose(TakeFirst(), remove_tags, lambda s: s.replace(u'于', '').strip())

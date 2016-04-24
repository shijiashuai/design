# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import hashlib
import logging
import types

from pyes import ES


class ElasticSearchPipeline(object):
    settings = None
    es = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        ext.settings = crawler.settings

        basic_auth = {}

        if ext.settings['ELASTICSEARCH_USERNAME']:
            basic_auth['username'] = ext.settings['ELASTICSEARCH_USERNAME']

        if ext.settings['ELASTICSEARCH_PASSWORD']:
            basic_auth['password'] = ext.settings['ELASTICSEARCH_PASSWORD']

        if ext.settings['ELASTICSEARCH_PORT']:
            uri = "%s:%d" % (ext.settings['ELASTICSEARCH_SERVER'],
                             ext.settings['ELASTICSEARCH_PORT'])
        else:
            uri = "%s" % (ext.settings['ELASTICSEARCH_SERVER'])

        ext.es = ES([uri], basic_auth=basic_auth)
        return ext

    def index_item(self, item):
        logger = logging.getLogger('elastic')
        if self.settings.get('ELASTICSEARCH_UNIQ_KEY'):
            uniq_key = self.settings.get('ELASTICSEARCH_UNIQ_KEY')
            local_id = hashlib.sha1(item[uniq_key]).hexdigest()
            logger.log(self.settings.get('ELASTICSEARCH_LOG_LEVEL'),
                       "Generated unique key %s" % local_id, )
            op_type = 'index'
        else:
            op_type = 'create'
            local_id = item['id']
        self.es.index(dict(item),
                      self.settings.get('ELASTICSEARCH_INDEX'),
                      self.settings.get('ELASTICSEARCH_TYPE'),
                      id=local_id,
                      op_type=op_type)

    def process_item(self, item, spider):
        if isinstance(item, types.GeneratorType) or isinstance(item,
                                                               types.ListType):
            for each in item:
                self.process_item(each, spider)
        else:
            logger = logging.getLogger('elastic')

            self.index_item(item)
            logger.log(self.settings.get('ELASTICSEARCH_LOG_LEVEL'),
                       "Item sent to Elastic Search %s"
                       % (self.settings.get('ELASTICSEARCH_INDEX')))
            return item

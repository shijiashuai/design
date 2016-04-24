from pyspark import SparkContext, SparkConf
from scrapy.cmdline import execute
import sys
import os
import zipfile


def f(_):
    # print os.getcwd()
    # print os.listdir('.')
    # zfile = zipfile.ZipFile('amazon_crawler.zip')
    # for filename in zfile.namelist():
    #     zfile.extract(filename, '.')
    execute(['', 'runspider', 'book_spider.py'])


sc = SparkContext('spark://shijiashuaideMacBook-Pro.local:7077')
sc.parallelize(["scala", '2']).foreach(f)

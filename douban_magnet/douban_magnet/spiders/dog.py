# -*- coding: utf-8 -*-
import scrapy
import re
from pymongo import MongoClient
from douban_magnet.items import DoubanMagnetItem

class ChookjiSpider(scrapy.Spider):
    name = 'doggou'
    allowed_domains = ['cilihu.xyz']
    start_urls = ['http://www.cilihu.xyz/']


    def __init__(self):
        self.url = 'http://www.cilihu.xyz/cilisousuo/'
        self.hand = 'magnet:?xt=urn:btih:'
        self.conn = MongoClient('mongodb://localhost:27017/')
        self.db = self.conn.config
        self.sow = None

    def start_requests(self):

        names = self.db.suspense.find({}, {'movie_name': 1, '_id': 0}).limit(5000).skip(5000)
        for name in names:
            url = self.url + name['movie_name'] + '.html'
            # url = self.url + 'ARM-500' + '.html'
            yield scrapy.Request(url, callback=self.parse_page, meta={'movie_name': name['movie_name']})

    def parse_page(self, response):
        movie_name = response.meta.get('movie_name')
        row = response.xpath("//div[@class='btsowlist']/div[@class='row'][1]/a/@href").get()
        if row:
            str = re.match('.*/(.*?).html', row).group(1)
            magnet = self.hand + str
            item = DoubanMagnetItem(magnet=magnet, movie_name=movie_name)
            yield item
        else:
            magnet = None
            item = DoubanMagnetItem (magnet=magnet, movie_name=movie_name)
            yield item
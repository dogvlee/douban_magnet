# coding=utf-8
# pymongo这个包是阻塞的操作
import pymongo

from twisted.internet import defer, reactor

import pymysql
from twisted.enterprise import adbapi
from pymysql import cursors

class MongoPiplineUpdate(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_col = 'suspense'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI', 'mongodb://127.0.0.1:27017/'),
            mongo_db=crawler.settings.get('MONGO_DB'),

        )

    def open_spider(self, spider):
        """
        爬虫启动时，启动
        :param spider:
        :return:
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.mongodb = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        爬虫关闭时执行
        :param spider:
        :return:
        """
        self.client.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        out = defer.Deferred()
        reactor.callInThread(self._update, item, out, spider)
        yield out
        defer.returnValue(item)

    def _update(self, item, out, spider):
        """
        插入函数
        :param item:
        :param out:
        :return:
        """
        # self.mongodb[self.mongo_col].insert(dict(item))
        self.mongodb[self.mongo_col].update_many({'movie_name':item['movie_name']},{'$set':{'magnet':item['magnet']}})
        # self.mongodb[self.mongo_col].update_many({'number': item['number']}, {'$push': {'magnet': item['magnet']}})
        # result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
        reactor.callFromThread(out.callback, item)


# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs,json
from datetime import datetime
import redis


class AccesshouseurlsPipeline(object):
    def __init__(self):
        self.time_now = datetime.now().strftime('%Y-%m-%d %H:%M')

    def open_spider(self,spider):
        filen = '/media/gumoha/资料/Scrapy/lianjia_houseUrls/Data/{0}-({1}).json'.format('ChengDu_ershoufang',self.time_now)
        self.file = codecs.open(filen,'w')

    def close_spider(self,item,spider):
        self.file.close()


    def process_item(self, item, spider):
        line = '{0}\n'.format(json.dumps(dict(item), ensure_ascii=False))
        self.file.write(line)

        return item



class RedisPipeline(object):
    def __init__(self):
        self.key = 'Lianjia_ChengDu_ershouHouseUrls'
        self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, password=None,
                                    encoding='utf-8', decode_responses=True)
        self.redisdb = redis.Redis(connection_pool=self.pool)


    def open_spider(self,spider):
        spider.clog.info('链接Redis:{0} 成功'.format(self.redisdb))


    def close_spider(self,item,spider):
        pass


    def process_item(self, item, spider):
        try:
            self.redisdb.sadd(self.key, json.dumps(item['houseUrl']))

        except Exception as e:
            spider.clog.error('存入失败！Redis_key{0}:{1},Error:{2}'.format(self.key,str(item['houseUrl']),e))

        return item


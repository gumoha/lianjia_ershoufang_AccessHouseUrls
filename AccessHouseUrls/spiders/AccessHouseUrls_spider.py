import scrapy
import time,random,json,logging,re
from datetime import datetime
from AccessHouseUrls.items import AccesshouseurlsItem
import redis
#from scrapy_redis.spiders import RedisSpider


class CustomLogger(object):
    def __init__(self, logger_name):
        filen = datetime.now().strftime('%Y-%m-%d %H')

        self.log_filen = r"/media/gumoha/资料/Scrapy/lianjia_houseUrls/Log/-log-{0}.json".format(filen)
        self.log_level = logging.INFO
        self.name = logger_name  #
        self.logger = logging.getLogger(self.name)

        # 定义输出格式
        log_format = '%(asctime)s-%(name)s-%(levelname)s--%(message)s'
        self.formatter = logging.Formatter(log_format)

        # 创建一个handler，用于输出到控制台
        self.sh = logging.StreamHandler()
        self.sh.setFormatter(self.formatter)

        # 创建一个handler，用于输出到日志文件
        self.logger.setLevel(self.log_level)
        self.fh = logging.FileHandler(self.log_filen, mode='a')
        self.fh.setFormatter(self.formatter)

        self.logger.addHandler(self.sh)
        self.logger.addHandler(self.fh)

    def getlog(self):
        return self.logger

class LjHouseUrls(scrapy.Spider):
    clog = CustomLogger('clog').getlog()

    name = 'Lianjia_CDesr_HouseUrls'
    allowed_domains = ['lianjia.com']

    headers_house = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip,deflate,br',
        'Connection': 'keep-alive',
        'Host': 'cd.lianjia.com',
        'DNT': '1',
        'Referer': 'https://cd.lianjia.com/',
        'Upgrade-Insecure-Requests': '1',
    }

    headers_blocks ={
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8  ',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip,deflate,br',
        'Connection': 'keep-alive',
        'Host': 'cd.lianjia.com',
        'DNT': '1',
        'Referer': 'https://cd.lianjia.com/',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control':'max-age=0',


    }


    def get_blocksUrls(self):
        try:
            with open('/media/gumoha/资料/Scrapy/lianjia_blocks/Chengdu_blocks_new.json', 'r') as f:
                lines = f.readlines()
                print('打开文件，获取Blocks_Urls')
                try:
                    for l in lines:
                        l = json.loads(l)
                        url = l['block_url']
                        yield url
                except Exception as e:
                    self.clog.error('读取url出错'.format(e))

        except Exception as e:
            self.clog.error('打开文件出错'.format(e))

    def start_requests(self):
        for url in self.get_blocksUrls():
            self.clog.info('开始搜索街区链接{0}'.format(url))
            yield scrapy.Request(url, callback=self.parse_pg, headers=self.headers_blocks)

    def parse_pg(self,response):
        pages = (int(
            response.xpath('//div[@class="resultDes clear"]/h2/span/text()').extract_first().strip()) // 30) + 2

        pg_url = '{0}pg'.format(response.url)
        for n in range(1, pages):
            next_url = '{0}{1}/'.format(pg_url, str(n))

            time.sleep(random.random()*10)
            try:
                self.clog.info('正在搜索获取{0}'.format(next_url))
                yield scrapy.Request(next_url, method='GET',callback=self.parse_houseUrl, headers=self.headers_house)
            except Exception as e:
                self.clog.error(e)

    def parse_houseUrl(self,response):
        time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        houseUrls = response.xpath(
            '//li[@class="clear LOGCLICKDATA"]/div[@class="info clear"]/div[@class="title"]/a/@href').extract()

        for url in houseUrls:
            item = AccesshouseurlsItem()

            self.clog.info('获取到的House_url{0}'.format(url))
            try:
                item['houseUrl'] = url
                item['datetime'] = time_now
                yield item
            except Exception as e:
                self.clog.error(e)





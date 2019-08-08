# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import fake_useragent


class SoufangDownloaderMiddleware(object):
    def __init__(self):
        self.UA = fake_useragent.UserAgent()

    def process_request(self, request, spider):
        request.headers.setdefault("User-Agent", self.UA.google)

from __future__ import absolute_import, division, unicode_literals

import logging
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

import twisted.internet.task
from twisted.internet import reactor

from fake_useragent import UserAgent

DELAY_META = '__defer_delay'

logger = logging.getLogger(__name__)

def defer_request(seconds, request):
    meta = dict(request.meta)
    meta.update({DELAY_META: seconds})
    return request.replace(meta=meta)

# https://github.com/scrapy/scrapy/issues/802#issuecomment-498749742
class DeferMiddleware(object):
    def process_request(self, request, spider):
        delay = request.meta.pop(DELAY_META, None)
        if not delay:
            return

        return twisted.internet.task.deferLater(reactor, delay, lambda: None)


class TooManyRequestsRetryMiddleware(RetryMiddleware):
    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler
        self.user_agent = UserAgent()
        self.current_agent = self.user_agent.random
        self.rotate_count = 0
        self.backoff_time = 0.5

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def rotate_agent(self, force = False):
        self.rotate_count += 1
        if force or self.rotate_count % 10 == 0:
            self.current_agent = self.user_agent.random        

    def process_request(self, request, spider):
        self.rotate_agent()
        request.headers.setdefault('User-Agent', self.current_agent)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            self.backoff_time = min(self.backoff_time * 1.5, 5)

            logger.warning("HTTP 429 received, backoff for %.2f seconds", self.backoff_time)

            key, slot = self._get_slot(request, spider)
            slot.delay = self.backoff_time

            return self.check_retry(request, response, spider)
        elif response.status in self.retry_http_codes:
            return self.check_retry(request, response, spider)

        self.backoff_time = 0.5
        return response

    def check_retry(self, request, response, spider):
        self.rotate_agent(True)
        reason = response_status_message(response.status)
        retry = self._retry(request, reason, spider)
        if retry:
            retry.headers['User-Agent'] = self.current_agent
            return retry
        
        return response

    def _get_slot(self, request, spider):
        key = request.meta.get('download_slot')
        return key, self.crawler.engine.downloader.slots.get(key)

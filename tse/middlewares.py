from __future__ import absolute_import, division, unicode_literals

import logging
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

import twisted.internet.task
from twisted.internet import reactor

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

# TODO: Allow bursting
# https://docs.aws.amazon.com/waf/latest/developerguide/waf-rule-statement-type-rate-based.html 
class TooManyRequestsRetryMiddleware(RetryMiddleware):
    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler
        self.backoff_time = 1
        self.response_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        self.response_count += 1

        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            logger.warning("HTTP 429 received, backoff for %.2f seconds, rc: %d", self.backoff_time, self.response_count)

            slot = self._get_slot(request, spider)
            slot.delay = self.backoff_time

            self.backoff_time = min(self.backoff_time * 1.5, 5)

            return self.check_retry(request, response, spider)
        elif response.status in self.retry_http_codes:
            return self.check_retry(request, response, spider)

        self.backoff_time = 1
        return response

    def check_retry(self, request, response, spider):
        reason = response_status_message(response.status)
        return self._retry(request, reason, spider) or response

    def _get_slot(self, request, spider):
        key = request.meta.get('download_slot')
        return self.crawler.engine.downloader.slots.get(key)

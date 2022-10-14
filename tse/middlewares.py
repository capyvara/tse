# https://github.com/scrapy/scrapy/issues/802#issuecomment-498749742

from __future__ import absolute_import, division, unicode_literals

import twisted.internet.task
from twisted.internet import reactor

DELAY_META = '__defer_delay'


def defer_request(seconds, request):
    meta = dict(request.meta)
    meta.update({DELAY_META: seconds})
    return request.replace(meta=meta)

class DeferMiddleware(object):
    def process_request(self, request, spider):
        delay = request.meta.pop(DELAY_META, None)
        if not delay:
            return

        return twisted.internet.task.deferLater(reactor, delay, lambda: None)
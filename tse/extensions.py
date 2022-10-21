import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from twisted.internet import task

logger = logging.getLogger(__name__)

class LogStatsDivulga:
    def __init__(self, stats, interval=60.0):
        self.stats = stats
        self.interval = interval
        self.multiplier = 60.0 / self.interval
        self.task = None

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat("LOGSTATS_INTERVAL")
        if not interval:
            raise NotConfigured
        o = cls(crawler.stats, interval)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)

    def log(self, spider):
        pending = self.stats.get_value("divulga/pending", 0)
        dupes = self.stats.get_value("divulga/dupes", 0)
        logger.info("- Pending: %(pending)d, dupes: %(pending)d", {"pending": pending, "dupes": dupes}, extra={"spider": spider})
        return

    def spider_closed(self, spider, reason):
        if self.task and self.task.running:
            self.task.stop()
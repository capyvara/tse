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
        pending = len(spider.pending) if hasattr(spider, "pending") else 0
        dupes = self.stats.get_value("divulga/dupes", 0)
        bumped = self.stats.get_value("divulga/bumped", 0)
        reindexes = self.stats.get_value("divulga/reindexes", 0)
        logger.info("Divulga - pending: %(pending)d, dupes: %(dupes)d, bumped: %(bumped)d, reindexes: %(reindexes)d", 
            {"pending": pending, "dupes": dupes, "bumped": bumped, "reindexes": reindexes}, 
            extra={"spider": spider})
        return

    def spider_closed(self, spider, reason):
        if self.task and self.task.running:
            self.task.stop()


class LogStatsUrna:
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
        sections = self.stats.get_value("urna/sections", 0)
        processed_sections = self.stats.get_value("urna/processed_sections", 0)
        ballot_box_files = self.stats.get_value("urna/ballot_box_files", 0)
        processed_ballot_box_files = self.stats.get_value("urna/processed_ballot_box_files", 0)

        logger.info("Urna - sections: %(sections)d, processed_sections: %(processed_sections)d, ballot_box_files: %(ballot_box_files)d, processed_ballot_box_files: %(processed_ballot_box_files)d", 
            {"sections": sections, 
            "processed_sections": processed_sections, 
            "ballot_box_files": ballot_box_files, 
            "processed_ballot_box_files": processed_ballot_box_files}, 
            extra={"spider": spider})
        return

    def spider_closed(self, spider, reason):
        if self.task and self.task.running:
            self.task.stop()            
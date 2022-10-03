# Scrapy settings for divulgacao project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "divulgacao"

LOG_LEVEL = "INFO"

SPIDER_MODULES = ["divulgacao.spiders"]
NEWSPIDER_MODULE = "divulgacao.spiders"

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 100
RETRY_TIMES = 5

FILES_STORE = "data/download"

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

HTTPCACHE_ENABLED = False

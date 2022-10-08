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

# Not needed for this scrap
HTTPCACHE_ENABLED = False
ROBOTSTXT_OBEY = False

# Where to put downloaded files
FILES_STORE = "data/download"

# How aggressive should the scrapping be done, watch out to not flood the server
CONCURRENT_REQUESTS = 200
CONCURRENT_REQUESTS_PER_DOMAIN = 200
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0

RETRY_TIMES = 5

# States to get information from, beware that without "br" some shared files such as config woudn't be downloaded
STATES = "br ac al am ap ba ce df es go ma mg ms mt pa pb pe pi pr rj rn ro rr rs sc se sp to zz"

DOWNLOAD_PICTURES=True

# Optional regex to filter filenames to narrow scope

# Examples
# Do not download signature files
# IGNORE_PATTERN=r"\.sig"

# Do not download signature nor vatiable files (too many are changed during the appuration process)
# IGNORE_PATTERN=r"\.sig|\-v.json"

# On urna download only logs (negative lookbehind)
# IGNORE_PATTERN=r".*(?<!\.logjez)$"


# Sim env
# HOST = "https://resultados-sim.tse.jus.br"
# ENVIRONMENT = "teste"
# CYCLE = "ele2022"
# ELECTIONS = [9240, 9238]
# PLEA = []


# Prod env
HOST = "https://resultados.tse.jus.br"
ENVIRONMENT = "oficial"
CYCLE = "ele2022"
# 1st round
PLEA = "406"
ELECTIONS = ["544", "546", "548"]
# 2nd round
# PLEA = "407"
# ELECTIONS = ["545", "547"]

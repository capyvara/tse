# Scrapy settings for tse project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "tse"

LOG_LEVEL = "INFO"

SPIDER_MODULES = ["tse.spiders"]
NEWSPIDER_MODULE = "tse.spiders"

# Not needed for this scrap
HTTPCACHE_ENABLED = False
ROBOTSTXT_OBEY = False
COOKIES_ENABLED = False
REFERER_ENABLED = False
RANDOMIZE_DOWNLOAD_DELAY = False
URLLENGTH_LIMIT = None
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
    'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': None,
    'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None,
}

# Where to put downloaded files
FILES_STORE = "data/download"

# Autothrottle will handle the actual concurrency 
# Keep min keep slots for reindexes: (num states * num elections) + auto target concurrency + slack
CONCURRENT_REQUESTS = 200
CONCURRENT_REQUESTS_PER_DOMAIN = 200

# How aggressive should the scrapping be done, watch out to not flood the server
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.01
AUTOTHROTTLE_TARGET_CONCURRENCY = 50.0

DOWNLOAD_TIMEOUT = 10
RETRY_TIMES = 5

# States to get information from, beware that without "br" some shared files such as config woudn't be downloaded
STATES = ["br", "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go", "ma", "mg", "ms", "mt", "pa", 
            "pb", "pe", "pi", "pr", "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to", "zz"]

# Download pictures of the candidates
DOWNLOAD_PICTURES = True

# Keep backups of files when overwritten (at .ver directories)
KEEP_OLD_VERSIONS = True

# Optional regex to filter filenames to narrow scope

# Examples
# Do not download signature files
# IGNORE_PATTERN = r"\.sig"

# Do not download signature nor variable files (too many are changed during the appuration process)
# IGNORE_PATTERN = r"\.sig|\-v.json"

# On urna download only logs (negative lookbehind)
# IGNORE_PATTERN = r".*(?<!\.logjez)$"


# Sim env
# HOST = "https://resultados-sim.tse.jus.br"
# ENVIRONMENT = "teste"
# CYCLE = "ele2022"
# ELECTIONS = ["9722", "9724"]
# PLEA = "8480"


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


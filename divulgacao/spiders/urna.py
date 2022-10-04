import os
import json
import datetime
import scrapy
import logging
import urllib.parse

from divulgacao.common.fileinfo import FileInfo

class UrnaSpider(scrapy.Spider):
    name = "urna"

    

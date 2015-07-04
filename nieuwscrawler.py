__author__ = 'roelvdberg@gmail.com'

import crawler.crawl as crawl
from model import Session

try:
    from settings import *
except ImportError:
    pass


session = Session()


class NieuwsCrawler(crawl.Crawler):
    headers = ["h" + str(i + 1) for i in range(6)]
    alt_headers = ["strong"]
    paragraph = ["p", "article"]
    description = ['html/head/meta[contains(@{attr}, "description")]'
                       .format(attr=x)
                   for x in ["prop", "property", "name"]]
    title = "title"
    time = (".//time", "datetime")

    def parse(self):
        # get title
        # TODO: use iterparse

        pass


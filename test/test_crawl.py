__author__ = 'roelvdberg@gmail.com'

import unittest

import lxml.etree as etree

import crawler.webpage

def fetch(self, url=None, download=True, *args, **kwargs):
    """
    Fetches the content of a file, based on a filepath(url).

    Changes fetch so that it loads a file from disk instead of from url.

    :param url: the path from which content will be downloaded.
    :param download: default: True, if set to False, the url content will
        not be downloaded. The parse method will look at the html content
        given on initialization.
    """
    if download and not self.html:
        if url is None:
            url = self.url
        with open(url, 'r') as response:
            self.html = response.read()
    self.parse(*args, **kwargs)

crawler.webpage.WebpageRaw.fetch = fetch
crawler.webpage.Webpage.fetch = fetch
crawler.webpage.Links.fetch = fetch

import crawler.validate as validate


def add_links(self, link_container, depth=0, base_url=None):
    """
    Add a list of urls to self at a certain depth.

    :param link_container: list of urls
    :param depth: depth at which the urls have been harvested
    :param base_url: base at which the urls have been harvested
    """
    if not base_url:
        base_url = self.base[0]
    try:
        for url_ in link_container.links:
            if "#" in url_:
                url_ = url_.split('#')[0]
                if not len(url_):
                    continue
            if not validate.url_explicit(url_):
                continue
            self.add(url_, depth)
    except AttributeError:
        pass

import logging
import threading
import dateutil.parser

try:
    import webpage
    from crawl import BaseUrl
    import robot
    import model
except ImportError:
    from crawler.crawl import BaseUrl
    import crawler.robot as robot
    import crawler.model as model

LOG_FILENAME = "testlog.log"

# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
print_logger = logging.StreamHandler()
print_logger.setLevel(logging.DEBUG)
logger.addHandler(print_logger)


def init(self, base, link_queue, historic_links, page=webpage.WebpageRaw,
             base_url=None, depth=0, database_lock=None):
    """
    :param base: base url string .
    :param link_queue: queue from base url.
    :param historic_links: historic links from base url.
    :param page: WebPage class or one of its children.
    :param base_url: BaseUrl object that at least contains this website.
    :param depth: crawl depth of this website.
    """
    if not database_lock:
        self.database_lock = threading.RLock()
    else:
        self.database_lock = database_lock
    self.session = model.Session()
    self.base = base
    self.has_content = True
    self.robot_txt = robot.Txt('/'.join([base, 'robots.txt']))
    self.robot_txt.read()
    if base_url:
        self.base_url = base_url
    else:
        self.base_url = BaseUrl(base, self.database_lock)
        _, _, links = self.base_url.base_queue.get()
    self.links = link_queue
    self.depth = depth
    try:
        print('trying to add sitemap')
        logger.debug('CHECK DIT' + str(self.robot_txt.sitemap.links))
        print('at least I tried', self.base)
        for i, link in enumerate(self.robot_txt.sitemap.links):
            if self.robot_txt.sitemap.xml:
                try:
                    site = self.session.query(model.Webpage).filter_by(
                        url=link).order_by(
                        model.Webpage.crawl_modified).all()[-1]
                    modified = dateutil.parser.parse(
                        self.robot_txt.sitemap.modified_time[i])
                    if site.crawl_modified > modified:
                        with base_url.lock:
                            self.links.put(link)
                except IndexError:
                    with base_url.lock:
                        self.links.put(link)
            else:
                with base_url.lock:
                    self.links.put(link)
        with base_url.lock:
            historic_links += self.robot_txt.sitemap.links
    except AttributeError:
        logger.debug('SITEMAP NOT FOUND FOR: ' + self.base)
    self.webpage = page



try:
    import sitemap
    import crawl
except ImportError:
    import crawler.sitemap as sitemap
    import crawler.crawl as crawl

crawl.Website.__init__ = init


def gz_file_fetch(self, url=None, download=None, *args, **kwargs):
    if not url:
        url = self.url
    with open(url, 'rb') as f:
        self.html = f.read()
    self.parse()

sitemap.GunZip.fetch = gz_file_fetch


try:
    from webpage import Webpage
    from crawl import Website
    from datetime_from_html import WebPageDateTime
    from test.webpage_testcases import *
except ImportError:
    from crawler.webpage import Webpage
    from crawler.crawl import Website
    from crawler.datetime_from_html import WebPageDateTime
    from test.webpage_testcases import *

with open('sitemapindexlocation.txt', 'r') as f:
    sitemapindexlocation = f.read()

sitemap_base = sitemapindexlocation.split('/')[:-1]

print(sitemapindexlocation)

sitemap.XmlSitemap.fetch = fetch
sitemap.XmlSitemapIndex.fetch = fetch
sitemap.XmlUrlset.fetch = fetch

class TestSitemap(unittest.TestCase):
    sitemap = sitemap.Sitemap(url=sitemapindexlocation, base_url='')

    def test_sitemap_load(self):
        print(self.sitemap.links)
#
#     def test_agent(self):
#         agent = self.webpage.agent
#         self.assertEqual(2, len(agent))
#         self.assertTrue('User-Agent' in agent[1].keys())
#         self.assertIsInstance(agent[0], bytes)
#

# TEST_URL = 'http://python.org/'
#
#
#
# class TestBaseFetcher(unittest.TestCase):
#     webpage = Webpage(TEST_URL)
#
#     def test_agent(self):
#         agent = self.webpage.agent
#         self.assertEqual(2, len(agent))
#         self.assertTrue('User-Agent' in agent[1].keys())
#         self.assertIsInstance(agent[0], bytes)
#
# class TestWebsite(unittest.TestCase):
#
#     def test_parse(self):
#         pass
#
# class TestDateTime(unittest.TestCase):
#
#     def setUp(self):
#         self.papers = papers
#         self.paper_trees = [(html, etree.HTML(html))for html in self.papers]
#         self.wpdt = WebPageDateTime()
#
#     def test_dt(self):
#         for html, htmltree in self.paper_trees:
#             self.wpdt.method(html, htmltree)
#
#     def tearDown(self):
#         pass


if __name__ == '__main__':
    unittest.main()
    # x = TestDateTime
    # x.setUp()
    # x.test()

__author__ = 'roelvdberg@gmail.com'

import unittest

import lxml.etree as etree

from crawler.webpage import Webpage
from crawler.crawl import Website
from crawler.datetime_from_html import WebPageDateTime
from test.testcases import *

TEST_URL = 'http://python.org/'

class TestBaseFetcher(unittest.TestCase):
    webpage = Webpage(TEST_URL)

    def test_agent(self):
        agent = self.webpage.agent
        self.assertEqual(2, len(agent))
        self.assertTrue('User-Agent' in agent[1].keys())
        self.assertIsInstance(agent[0], bytes)

class TestWebsite(unittest.TestCase):

    def test_parse(self):
        pass

class TestDateTime(unittest.TestCase):

    def setUp(self):
        self.papers = papers
        self.paper_trees = [(html, etree.HTML(html))for html in self.papers]
        self.wpdt = WebPageDateTime()

    def test_dt(self):
        for html, htmltree in self.paper_trees:
            self.wpdt.method(html, htmltree)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
    x = TestDateTime
    x.setUp()
    x.test()

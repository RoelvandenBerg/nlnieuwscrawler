__author__ = 'roelvdberg@gmail.com'

import unittest

from crawler.crawl import Webpage
from crawler.crawl import Website


TEST_URL = 'http://python.org/'

class TestBaseFetcher(unittest.TestCase):
    fetcher = Webpage(TEST_URL)

    def test_crawl(self):
        self.assertRaises(NotImplementedError, self.fetcher.fetch)
        self.assertIsInstance(self.fetcher.html, bytes)

    def test_agent(self):
        agent = self.fetcher.agent
        self.assertEqual(2, len(agent))
        self.assertTrue('User-Agent' in agent[1].keys())
        self.assertIsInstance(agent[0], bytes)

class TestWebsite(Website):

    def test_parse(self):
        pass



if __name__ == '__main__':
    unittest.main()

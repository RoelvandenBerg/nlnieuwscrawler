__author__ = 'roelvdberg@gmail.com'

import copy
import datetime.datetime as dt
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser

from lxml import etree

from crawler.model import Session
from crawler.model import Paragraph
from crawler.settings import *


class Fetcher(object):
    """
    Fetches site content from an [url]
    """
    tag = ""
    name = "untitled"
    attr = None
    tags = None

    def __init__(self, url):
        self.url = url

    def fetch(self, url=None, *args, **kwargs):
        if url is None:
            url = self.url
        data, headers = self.agent
        with request.urlopen(request.Request(url, data, headers)) as response:
            self.html = response.read()
        self.parse(*args, **kwargs)

    @property
    def agent(self):
        data = urllib.parse.urlencode(USER_AGENT_INFO)
        data = data.encode('utf-8')
        headers = {'User-Agent': USER_AGENT}
        return data, headers

    def parse(self, within_element=None, method="xpath"):
        self.trees = [etree.HTML(self.html)]
        if within_element:
            self.trees = self._fetch_by_method(within_element, method)
        parse_iterator = (self._get_attr(y) for x in self.trees
                          for y in x.iter(self.tag))
        content = self._parse_edit(parse_iterator)
        if self.name:
            setattr(self, self.name, parsed)
        else:
            self.content = content

    def _parse_edit(self, iterator):
        return list(iterator)

    def _fetch_by_method(self, within_element, method):
        method = getattr(self.tree, "get_element_by_" + method)
        return [x for tree in self.trees for x in method(within_element)]

    def _get_attr(self, y):
        try:
            return y.attrib[self.attr]
        except AttributeError:
            return self._textwalk(y)

    def _textwalk(self, element):
        children = [self._textwalk(x) + x.tail for x in element]
        return element.text + "".join(children)


class ParagraphFetcher(Fetcher):
    tag = "p"
    name = "text"

    def __init__(self):
        super().__init__()
        self.session = Session()

    def store_content(self, baseurl):
        for paragraph in self.content:
            new_item = Paragraph(
                name='new person',
                datetime=dt.now(),
                site=self.url,
                paragraph=paragraph,
                url=self.url
            )


class LinkFetcher(Fetcher):
    tag = "a"
    attr = "href"
    name = "links"


class Sitemap(LinkFetcher):
    visited = []

    def __init__(self, url):
        self.url = url
        self.parse()

    def _parse_edit(self, iterator):
        return iterator

    def __add__(self, other):
        sum_ = copy.deepcopy(self)
        sum_.visited += other.visited
        sum_.links += other.links
        return sum_

    def __iter__(self):
        for link in self.links:
            if not link in self.visited:
                yield link
                self.visited.append(link)


class RobotTxt(robotparser.RobotFileParser):
    """
    Extention of robotparser, adds sitemap functionality, mainly a copy.
    """

    def parse(self, lines):
        """Parse the input lines from a robots.txt file.

        We allow that a user-agent: line is not preceded by
        one or more blank lines.
        """
        # states:
        #   0: start state
        #   1: saw user-agent line
        #   2: saw an allow or disallow line
        state = 0
        entry = robotparser.Entry()

        for line in lines:
            if not line:
                if state == 1:
                    entry = robotparser.Entry()
                    state = 0
                elif state == 2:
                    self._add_entry(entry)
                    entry = robotparser.Entry()
                    state = 0
            # remove optional comment and strip line
            i = line.find('#')
            if i >= 0:
                line = line[:i]
            line = line.strip()
            if not line:
                continue
            line = line.split(':', 1)
            if len(line) == 2:
                line[0] = line[0].strip().lower()
                line[1] = urllib.parse.unquote(line[1].strip())
                if line[0] == "user-agent":
                    if state == 2:
                        self._add_entry(entry)
                        entry = robotparser.Entry()
                    entry.useragents.append(line[1])
                    state = 1
                elif line[0] == "disallow":
                    if state != 0:
                        entry.rulelines.append(robotparser.RuleLine(line[1],
                                                                    False))
                        state = 2
                elif line[0] == "allow":
                    if state != 0:
                        entry.rulelines.append(robotparser.RuleLine(line[1],
                                                                    True))
                        state = 2
                elif line[0] == 'sitemap':
                    if self.sitemap:
                        self.sitemap += Sitemap(line[1])
                    else:
                        self.sitemap = Sitemap(line[1])
        if state == 2:
            self._add_entry(entry)


class Crawler(LinkFetcher):
    def __init__(self, url, fetcher=ParagraphFetcher):
        super().__init__(url)
        self.robot = RobotTxt(url)
        self.links += self.robot.sitemap.links
        self.fetcher = fetcher
        self.content = []

    def _parse_edit(self, iterator):
        return [url for url in iterator if self._can_fetch(url)]

    def _can_fetch(self, url):
        return self.robot.can_fetch(url)

    def __iter__(self):
        self.parse()
        while len(self.links) > 0:
            link = self.links.pop()
            fetcher = self.fetcher(link)
            try:
                fetcher.store_content(link)
                self.content.append(fetcher.content)
            except AttributeError:
                fetcher.content = None
            yield link, fetcher.content
            self.visited.append(link)


if __name__ == "__main__":
    python_crawler = Crawler(BASE_URL)
    for i in range(10):
        url, content = python_crawler.next()
        print(url, content)
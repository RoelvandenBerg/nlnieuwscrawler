__author__ = 'roelvdberg@gmail.com'

import copy
from datetime import datetime as dt
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser

from lxml import etree

from crawler.model import Session
from crawler.model import Paragraph
from crawler.settings import *


def stringify(string):
    if string:
        return str(string)
    else:
        return ""


class Fetcher(object):
    """
    Fetches site content from an [url]
    """
    tag = ""
    name = None
    attr = None
    tags = None

    def __init__(self, url, html=None, base_url=None):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = url
        self.url = url
        self.html = html
        self.fetch()

    def fetch(self, url=None, download=False, *args, **kwargs):
        if self.html and not download:
            return
        if url is None:
            url = self.url
        data, header = self.agent
        with request.urlopen(request.Request(url, headers=header)) as response:
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
            setattr(self, self.name, content)
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
        except TypeError:
            return self._textwalk(y)

    def _textwalk(self, element):
        children = [self._textwalk(x) + stringify(x.tail) for x in element]
        return stringify(element.text) + "".join(children)


class ParagraphFetcher(Fetcher):
    tag = "p"

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html, base_url)
        self.session = Session()

    def store_content(self):
        for paragraph in self.content:
            new_item = Paragraph(
                datetime=dt.now(),
                site=self.base_url,
                paragraph=paragraph,
                url=self.url
            )
            self.session.add(new_item)
            self.session.commit()


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

    def __init__(self, url):
        self.sitemap = None
        super().__init__(url)

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
        self.robot = RobotTxt(urllib.parse.urljoin(url, 'robots.txt'))
        self.robot.read()
        self.links = []
        super().__init__(url)
        self.add_links(self.robot.sitemap)
        self.fetcher = fetcher
        self.content = []
        self.visited = []

    def add_links(self, link_container, depth=0):
        try:
            self.links += self._parse_edit(link_container.links, depth)
        except AttributeError:
            pass

    def _parse_edit(self, iterator, base_depth=0):
        result = []
        increased_base_depth = base_depth + 1
        for url in iterator:
            if url.startswith(self.base_url):
                depth = base_depth
            else:
                depth = increased_base_depth
            if self._can_fetch(url) and depth <= CRAWL_DEPTH:
                result.append((url, depth))
        return result

    def _can_fetch(self, url):
        return self.robot.can_fetch(USER_AGENT, url)

    def __iter__(self):
        while len(self.links) > 0:
            link, current_depth = self.links.pop()
            if link[0] == "#":
                continue
            if not "http" in link:
                link = urllib.parse.urljoin(self.url, link)
            fetcher = self.fetcher(url=link, base_url=self.url)
            fetcher.store_content()
            urlfetcher = LinkFetcher(link, fetcher.html)
            self.add_links(urlfetcher)
            try:


                self.content.append(fetcher.content)
            except AttributeError:
                print('error')
                fetcher.content = None
            yield link, fetcher.content
            self.visited.append(link)


# TODO: Check if robotparser requires direct link to robots.txt
# TODO: Find out what data / is acceptable for as useragent info
# TODO: Crawl delay functionality

if __name__ == "__main__":
    python_crawler = Crawler(BASE_URL)
    for url, content in python_crawler:
        print(url, content)
    print(python_crawler.links, python_crawler.visited)


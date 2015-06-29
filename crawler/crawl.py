__author__ = 'roelvdberg@gmail.com'

from urllib import parse
from urllib import robotparser
import urllib.request as request

from lxml import etree


USER_AGENT = '_'
USER_AGENT_INFO = {
          'name' : 'python crawler',
          'organisation': '-',
          'location' : 'Unknown',
          'language' : 'Python 3'
}


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
        entry = Entry()

        for line in lines:
            if not line:
                if state == 1:
                    entry = Entry()
                    state = 0
                elif state == 2:
                    self._add_entry(entry)
                    entry = Entry()
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
                        entry = Entry()
                    entry.useragents.append(line[1])
                    state = 1
                elif line[0] == "disallow":
                    if state != 0:
                        entry.rulelines.append(RuleLine(line[1], False))
                        state = 2
                elif line[0] == "allow":
                    if state != 0:
                        entry.rulelines.append(RuleLine(line[1], True))
                        state = 2
                elif line[0] == 'sitemap':
                    self.sitemap = Sitemap(line[1])
        if state == 2:
            self._add_entry(entry)


class BaseCrawler(object):
    """
    Fetches site content from an [url]
    """
    tag = ""
    name = "untitled"
    attr = None
    tags = None

    def __init__(self, url):
        self.url = url

    def crawl(self, url=None, *args, **kwargs):
        if url == None:
            url = self.url
        data, headers = self._set_agent()
        with request.urlopen(request(url, data, headers)) as response:
            self.html = response.read()
        self.parse(*args, **kwargs)

    def _set_agent(self):
        data = parse.urlencode(USER_AGENT_INFO)
        data = data.encode('utf-8')
        headers = {'User-Agent': USER_AGENT}
        return data, headers

    def parse(self, *args, **kwargs):
        pass


class Crawler(BaseCrawler):
    """
    Fetches and parses site content from an [url]
    """

    def parse(self, within_element=None, method="xpath"):
        self.trees = [etree.HTML(self.html)]
        if within_element:
            self.trees = self._grab_by_method(within_element, method)
        parse_iterator = (self._get_attr(y) for x in self.trees
                          for y in x.iter(self.tag))
        parsed = self._parse_edit(parse_iterator)
        setattr(self, name, parsed)

    def _parse_edit(self, iterator):
        return list(iterator)

    def _grab_by_method(self, within_element, method):
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

    def can_fetch(self):
        return True


class LinkCrawler(Crawler):
    tag = "a"
    attr = "href"
    name = "links"

    def __init__(self, url):
        super().__init__(url)
        self.robot = RobotTxt(url)

    def _parse_edit(self, iterator):
        return [x for x in iterator if self.can_fetch(x)]

    def can_fetch(self, url):
        return self.robot.can_fetch(url)

    def __iter__(self):
        self.parse()
        for site in self.robot.sitemap:
            # TODO
            yield site


class Sitemap(LinkCrawler):
    visited = []

    def __init__(self, url):
        self.url = url
        self.parse()

    def _parse_edit(self, iterator):
        return iterator

    def __iter__(self):
        for link in self.links:
            yield link
            self.visited.append(link)

class SimpleTextCrawl(Crawler):
    tag = "p"
    name = "text"

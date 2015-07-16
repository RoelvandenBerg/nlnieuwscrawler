__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
from time import sleep
import copy
import re
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser

from lxml import etree

from crawler.model import Session
from crawler.model import Paragraph
from crawler.model import create_all
from crawler.settings import *


class Head(object):
    location = "html/head"
    tags = {
        "title": "title",
        "base": "base_url",
        "meta": (
            ("name", {
                "keywords": "keywords",
                "description": "description",
                "author": "author",
                "revisit-after": "revisit-after",
                "robots": "robots"
            }),
            ("property", {
                "og:description": "description",
                "og:title": "title",
                "article:published_time": "time",
                "article:modified_time": "time",
                "article:expiration_time": "expiration_time",
                "article:author": "author",
                "article:section": "section",
                "article:tag": "tag"
            }),
        )
    }

    def __init__(self, htmltree):
        self.root = htmltree.xpath(self.location)

    def parse(self):
        for el in self.root:
            self.search(el)

    def search(self, element):
        key = element.tag
        skip_once = False
        try:
            result = self.tags[key]
        except KeyError:
            return
        for attribute, dictionary in result:
            try:
                attr_value = element.get(attribute)
                name = dictionary[attr_value]
                value = element.get("content")
                # if ',' in value:
                #     value = [x.strip() for x in value.split(',')]
                skip_once = True
                break
            except ValueError:
                name = result
                value = element.text
        try:
            getattr(self, name)
            if skip_once:
                return
        except AttributeError:
            pass
        setattr(self, name, value)

    # def parse_robots(self):
    #     {
    #         "noarchive":,
    #         "nosnippet": ,
    #         "noindex": ,
    #         "nofollow": ,
    #         "noimageindex":
    #     })


def stringify(string):
    if string:
        return str(string)
    else:
        return ""


class Website(object):
    """

    Fetches site content from an [url] and parses its contents.

    This is a base class that can be
    overwritten. Minimally one should overwrite
    """
    tag = ""
    name = None
    attr = None
    tags = None

    def __init__(self, url, html=None, base_url=None, *args, **kwargs):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = url
        self.url = url
        self.html = html
        self.fetch(*args, **kwargs)
        self.head = Head(self.base_tree)

    def fetch(self, url=None, download=True, *args, **kwargs):
        if download and not self.html:
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
        self.base_tree = etree.HTML(self.html)
        self.trees = [self.base_tree]
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
        except KeyError:
            return ""

    def _textwalk(self, element):
        children = [self._textwalk(x) + stringify(x.tail) for x in element]
        return stringify(element.text) + "".join(children)


class WebsiteText(Website):
    tag = "p"
    name = "text"

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.session = Session()

    def add_existing(self, attr):
        try:
            return self.head[attr]
        except:
            return None

    def store_content(self):
        text = [x for x in [txt.strip(' \t\n\r') for txt in self.text] if x != ""]
        for paragraph in text:
            new_item = Paragraph(
                crawl_datetime=dt.now(),
                site = self.add_existing("base_url"),
                title = self.add_existing("title"),
                description = self.add_existing("description"),
                author = self.add_existing("author"),
                published_time = self.add_existing("time"),
                expiration_time = self.add_existing("expiration_time"),
                section = self.add_existing("section"),
                tag = self.add_existing("tag"),
                paragraph = paragraph,
                url = self.url
            )
            self.session.add(new_item)
            self.session.commit()


class WebsiteLinks(Website):
    tag = "a"
    attr = "href"
    name = "links"

    def _parse_edit(self, iterator):
        return [link for link in iterator if url_validate(link)]


class Sitemap(WebsiteLinks):
    visited = []

    def __init__(self, url):
        self.url = url
        self.parse()

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
        self.crawl_delay = CRAWL_DELAY
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
                elif line[0].lower().startswith('crawl-delay'):
                    new_delay = int(line[1])
                    if self.crawl_delay < new_delay:
                        self.crawl_delay = new_delay
        if state == 2:
            self._add_entry(entry)


class EmptyWebsite(object):
    """
    Empty Website class that can serve as a dummy class for Crawler.
    """

    def __init__(self, *args, **kwargs):
        self.content = None


class Crawler(WebsiteLinks):
    def __init__(self, url, website=WebsiteText):
        self.robot = RobotTxt(urllib.parse.urljoin(url, 'robots.txt'))
        self.robot.read()
        self.links = []
        super().__init__(url)
        self.add_links(self.robot.sitemap)
        self.website = website
        self.content = []
        self.visited = []

    def add_links(self, link_container, depth=0):
        try:
            links = self._parse_edit(link_container.links, depth)
            self.links += links
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

    def _can_fetch(self, url_):
        return self.robot.can_fetch(USER_AGENT, url_)

    def __iter__(self):
        i = 0
        while len(self.links) > 0:
            i += 1
            link, current_depth = self.links.pop()
            if link[0] == "#":
                continue
            if not "http" in link:
                link = urllib.parse.urljoin(self.url, link)
            try:
                website = self.website(url=link, base_url=self.url)
            except urllib.error.HTTPError:
                continue
            try:
                head_robots = website.head.robots.lower()
            except AttributeError:
                head_robots=""
            if not "nofollow" in head_robots:
                urlfetcher = WebsiteLinks(url=link, base_url=self.url,
                                          html=website.html, download=False)
                self.add_links(urlfetcher)
            website.content = None
            if not any(robotsetting in head_robots for robotsetting in
                       ("noarchive", "nosnippet", "noindex")):
                try:
                    website.store_content()
                    self.content.append(website.content)
                except (TypeError, AttributeError):
                    print('Cannot store website to database.')

            yield link, website.content
            self.visited.append(link)
            sleep(self.robot.crawl_delay)


url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def url_validate(url):
    match = url.startswith("http") and \
            (url[-3] == "htm" or not url[-4] == ".") \
            and not 'twitter' in url
    return match


def url_validate_explicit(url):
    match = bool(url_regex.search(url)) and (url[-3] == "htm"
                                             or not url[-4] == ".")
    return match

# TODO: Check if robotparser requires direct link to robots.txt
# TODO: Find out what data / is acceptable for as useragent info
# TODO: Test crawl delay functionality
# TODO: ?skip urls in database? > Later
# TODO: add docstrings

if __name__ == "__main__":
    python_crawler = Crawler(BASE_URL)
    if RESET_DATABASE:
        create_all()

    for url, content in python_crawler:
        print(url)
    print(python_crawler.links, python_crawler.visited)

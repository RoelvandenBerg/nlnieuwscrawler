__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
import copy
import re
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser
import time

from lxml import etree

from crawler.model import Session
from crawler.model import Paragraph
from crawler.model import create_all
from crawler.settings import *


class Head(object):
    location = "/html/head"
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
        try:
            self.root = htmltree.xpath(self.location)[0]
        except IndexError:
            self.root = []

    def parse(self):
        for el in self.root:
            self.search(el)

    def find_name_value(self, result, element):
        name, value = None, None
        skip_once = False
        for attribute, dictionary in result:
            attr_value = element.get(attribute)
            if attr_value:
                try:
                    name = dictionary[attr_value]
                except KeyError:
                    continue
                value = element.get("content")
                skip_once = True
                break
                # if ',' in value:
                #     value = [x.strip() for x in value.split(',')]
        return name, value, skip_once

    def search(self, element):
        key = element.tag
        try:
            result = self.tags[key]
        except KeyError:
            return
        try:
            name, value, skip_once = self.find_name_value(result, element)
            if not skip_once:
                return
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
    robot_archive_options = ("noarchive", "nosnippet", "noindex")

    def __init__(self, url, html=None, base_url=None, *args, **kwargs):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = url
        self.url = url
        self.html = html
        self.fetch(*args, **kwargs)
        self.head = Head(self.base_tree)
        self.head.parse()

    def fetch(self, url=None, download=True, *args, **kwargs):
        if download and not self.html:
            if url is None:
                url = self.url
            data, header = self.agent
            with request.urlopen(request.Request(url, headers=header)) \
                    as response:
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

    @property
    def head_robots(self):
        try:
            return self.head.robots.lower()
        except AttributeError:
            return ""

    @property
    def follow(self):
        return not "nofollow" in self.head_robots

    @property
    def archive(self):
        return not any(robotsetting in self.head_robots for robotsetting in
                       self.robot_archive_options)


class WebsiteText(Website):
    tag = "p"
    name = "text"

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.session = Session()

    def add_existing(self, attr):
        try:
            return getattr(self.head, attr)
        except:
            return None

    def store_content(self):
        text = [x for x in [txt.strip(' \t\n\r')
                            for txt in self.text] if x != ""]
        datetimenow = dt.now()
        for paragraph in text:
            new_item = Paragraph(
                crawl_datetime=datetimenow,
                site=self.add_existing("base_url"),
                title=self.add_existing("title"),
                description=self.add_existing("description"),
                author=self.add_existing("author"),
                published_time=self.add_existing("time"),
                expiration_time=self.add_existing("expiration_time"),
                section=self.add_existing("section"),
                tag=self.add_existing("tag"),
                keywords=self.add_existing("keywords"),
                paragraph=paragraph,
                url=self.url
            )
            self.session.add(new_item)
        self.session.commit()
        self.content = ("Total paragraphs added: {n} @{dt}."
                        .format(n=len(text), dt=datetimenow))


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


class BaseUrl(list):
    """
    [BASE_URL, []n=1, []n=2, ..., []n=CRAWL_DEPTH]
    """

    def __init__(self, base):
        super().__init__()
        self.base = base
        self += [[base]] + [[] for _ in range(CRAWL_DEPTH)]

    def find(self, p_object, current_depth):
        if p_object.startswith(self.base):
            return self.base, 0
        for i, l in enumerate(self):
            for base in l:
                if p_object.startswith(base):
                    return base, i
        new_depth = current_depth + 1
        self.append(p_object, new_depth)
        return p_object, new_depth

    def append(self, p_object, depth):
        if depth > CRAWL_DEPTH \
                or p_object in self[depth] \
                or not url_validate_explicit(p_object):
            return False
        self[depth].append(p_object)
        return True

    def __str__(self):
        return self.base


class Crawler(object):

    def __init__(self, url, website=WebsiteText, base_url=None):
        self.robot = RobotTxt(urllib.parse.urljoin(url, 'robots.txt'))
        self.robot.read()
        self.links = [(url, 0)]
        self.visited = []
        self.broken = {}
        if base_url:
            self.base_url = BaseUrl(base_url)
        else:
            self.base_url = BaseUrl(url)
        try:
            self.add_links(self.robot.sitemap.links)
        except AttributeError:
            pass
        self.website = website
        self.content = []

    def add_links(self, link_container, depth=0, base_url=None):
        try:
            links = self._check_links(link_container.links, depth, base_url)
            self.links += links
        except AttributeError:
            pass

    def _check_links(self, urls, base_depth=0, base_url=None):
        result = []
        if not base_url:
            base_url = self.base_url.base
        for url_ in urls:
            if "#" in url_:
                url_ = url_.split('#')[0]
                if not len(url_):
                    continue
            if not url_.startswith("http"):
                url_ = urllib.parse.urljoin(base_url, url_)
            base, depth = self.base_url.find(url_, base_depth)
            if url_ in self.visited \
                    or (url_, depth) in self.links \
                    or (url_, depth) in result \
                    or not url_validate_explicit(url_):
                continue
            if self._can_fetch(url_) and depth <= CRAWL_DEPTH:
                result.append((url_, depth))
        return result

    def _can_fetch(self, url_):
        return self.robot.can_fetch(USER_AGENT, url_)

    def __iter__(self):
        while len(self.links) > 0:
            yield self._iter_once()

    def _iter_once(self):
        start_time = time.time()
        link, current_depth = self.links.pop()
        self.visited.append(link)
        current_base, _ = self.base_url.find(link, current_depth)

        try:
            website = self.website(
                url=link,
                base_url=current_base
            )
        except (urllib.error.HTTPError, UnicodeEncodeError):
            return None, None

        if website.follow:
            urlfetcher = WebsiteLinks(url=link, base_url=current_base,
                                      html=website.html, download=False)
            self.add_links(urlfetcher, current_depth, current_base)
        website.content = None

        if website.archive:
            try:
                website.store_content()
                self.content.append(website.content)
            except (TypeError, AttributeError):
                pass
        if VERBOSE:
            print(website.content)

        while (time.time() - start_time) < self.robot.crawl_delay:
            pass

        return link, website.content



NOFOLLOW = [
    "facebook",
    "google",
    "twitter",
    "youtube",
    "wikipedia",
    "linkedin",
    "creativecommons",
    'sciencecommons',
    "flickr",
    "wikimedia",
    "openstreetmap",
    "instagram",
    "github",
    "last.fm",
    "feedly",
    "mozzila",
    "opera"
]


def url_validate(url):
    url = url.strip(r'/')
    try:
        docxtest = not (url[-1]=='x' and url[-5] == ".")
    except IndexError:
        docxtest = True
    try:
        match = (url[-3] == "htm" or not url[-4] == ".") \
            and docxtest \
            and not any(nofollowtxt in url for nofollowtxt in NOFOLLOW)
    except IndexError:
        return True
    return match


url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def url_validate_explicit(url):
    match = bool(url_regex.search(url)) and url_validate(url)
    return match

# TODO: Check if robotparser requires direct link to robots.txt
# TODO: Find out what data / is acceptable for as useragent info
# TODO: ?skip urls in database? > Later
# TODO: add docstrings
# TODO: improve by threading (for example using gevent: http://www.gevent.org/)

if __name__ == "__main__":
    print(BASE_URL)
    standalone_crawler = Crawler(BASE_URL)
    if RESET_DATABASE:
        create_all()

    for url, content in standalone_crawler:
        print(url)
    print(standalone_crawler.links, standalone_crawler.visited)
    print(list(standalone_crawler.base_url))

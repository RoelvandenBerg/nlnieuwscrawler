__author__ = 'roelvdberg@gmail.com'

import copy
from datetime import datetime as dt
import queue
import re
import threading
import time
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser

from lxml import etree
from dateutil import parser as dtparser

from crawler.model import Session
from crawler.model import Paragraph
from crawler.settings import *


url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


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


def url_validate_explicit(url):
    match = bool(url_regex.search(url)) and url_validate(url)
    return match


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
        for attribute, dictionary in result:
            attr_value = element.get(attribute)
            if attr_value:
                try:
                    name = dictionary[attr_value]
                except KeyError:
                    continue
                value = element.get("content")
                return name, value, True
        return None, None, False

    def search(self, element):
        key = element.tag
        skip_once = False
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


class Webpage(object):
    """

    Fetches site content from an [url] and parses its contents.

    This is a base class that can be
    overwritten. Minimally one should overwrite
    """
    tag = ""
    name = None
    attr = None
    tags = None
    head = True
    robot_archive_options = ("noarchive", "nosnippet", "noindex")
    parser = etree.HTML

    def __init__(self, url, html=None, base_url=None, *args, **kwargs):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = url
        self.url = url
        self.html = html
        self.fetch(*args, **kwargs)
        if self.head:
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

    def parse(self, within_element=None, method="xpath"):
        self.base_tree = self.parser(self.html)
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
    def agent(self):
        data = urllib.parse.urlencode(USER_AGENT_INFO)
        data = data.encode('utf-8')
        headers = {'User-Agent': USER_AGENT}
        return data, headers

    @property
    def head_robots(self):
        try:
            return self.head.robots.lower()
        except AttributeError:
            return ""

    @property
    def followable(self):
        return not "nofollow" in self.head_robots

    @property
    def archivable(self):
        return not any(robotsetting in self.head_robots for robotsetting in
                       self.robot_archive_options)


class WebpageText(Webpage):
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
        pub_timestring = self.add_existing("time")
        published_time = dtparser.parse(pub_timestring, dayfirst=True)
        for paragraph in text:
            new_item = Paragraph(
                crawl_datetime=datetimenow,
                site=self.add_existing("base_url"),
                title=self.add_existing("title"),
                description=self.add_existing("description"),
                author=self.add_existing("author"),
                published_time=published_time,
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


class WebpageLinks(Webpage):
    tag = "a"
    attr = "href"
    name = "links"

    def _parse_edit(self, iterator):
        return [link for link in iterator if url_validate(link)]


class SitemapMixin(object):

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.visited = []
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

class HTMLSitemap(SitemapMixin, WebpageLinks):
    pass


class XMLSitemap(SitemapMixin, Webpage):
    tag = "loc"
    name = "links"
    parser = etree.XML


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
                    sitemap_url = line[1]
                    if sitemap_url.endswith('.xml'):
                        sitemap_class = XMLSitemap
                    else:
                        sitemap_class = HTMLSitemap
                    if self.sitemap:
                        print("    ADDING SITEMAP", sitemap_url)
                        self.sitemap += sitemap_class(sitemap_url)
                    else:
                        print("LOADING SITEMAP", sitemap_url)
                        self.sitemap = sitemap_class(sitemap_url)
                elif line[0].lower().startswith('crawl-delay'):
                    new_delay = float(line[1])
                    if self.crawl_delay < new_delay:
                        self.crawl_delay = new_delay
        if state == 2:
            self._add_entry(entry)


class EmptyWebpage(object):
    """
    Empty webpage class that can serve as a dummy class for Crawler.
    """

    def __init__(self, *args, **kwargs):
        self.content = None


class BaseUrl(list):
    """
    [BASE_URL, []n=1, []n=2, ..., []n=CRAWL_DEPTH]
    """
    base_regex = re.compile(r"^(?:http)s?://[\w\-_\.]+", re.IGNORECASE)

    def __init__(self, base):
        super().__init__()
        self += [[] for _ in range(CRAWL_DEPTH + 1)]
        self.lock = threading.RLock()
        self.base_queue = queue.Queue()
        if isinstance(base, str):
            base = [base]
        for base_url in base:
            self.append(base_url, 0)

    def add(self, p_object, current_depth):
        with self.lock:
            for i, l in enumerate(self):
                for base, link_history, link_queue in l:
                    if p_object.startswith(base):
                        if not p_object in link_history:
                            link_history.append(p_object)
                            link_queue.put(p_object)
                        return
            current_depth += 1
            self.append(p_object, current_depth)

    def append(self, p_object, depth):
        if depth > CRAWL_DEPTH:
            return
        with self.lock:
            if p_object not in self[depth] or url_validate_explicit(p_object):
                base = self.base_regex.findall(p_object)[0] + "/"
                link_queue = queue.Queue()
                link_queue.put(p_object)
                historic_links = [p_object]
                if ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE:
                    link_queue.put(base)
                    historic_links.append(base)
                self[depth].append((p_object, historic_links, link_queue))
                self.base_queue.put((base, depth, historic_links, link_queue))

    def add_links(self, link_container, depth=0, base_url=None):
        if not base_url:
            base_url = self.base
        try:
            for url_ in link_container.links:
                if "#" in url_:
                    url_ = url_.split('#')[0]
                    if not len(url_):
                        continue
                if not url_.startswith("http"):
                    url_ = urllib.parse.urljoin(base_url, url_)
                if not url_validate_explicit(url_):
                    continue
                self.add(url_, depth)
        except AttributeError:
            pass

    def __str__(self):
        return '"' + '", "'.join(self.base) + '"'

    @property
    def base(self):
        return self[0][0][0]

    @property
    def has_content(self):
        try:
            return any(any(url[2].qsize() > 0 for url in urls_at_one_depth)
                       for urls_at_one_depth in self)
        except IndexError:
            return False


class Website(object):

    def __init__(self, base, link_queue, historic_links, webpage=WebpageText,
                 base_url=None, depth=0, semaphore=None):
        self.base = base
        self.has_content = True
        self.robot = RobotTxt(urllib.parse.urljoin(base, 'robots.txt'))
        self.robot.read()
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = BaseUrl(base)
            _, _, links = self.base_url.base_queue.get()
        self.links = link_queue
        self.depth = depth
        try:
            for link in self.robot.sitemap.links:
                self.links.put(link)
            with base_url.lock:
                historic_links += self.robot.sitemap.links
        except AttributeError:
            pass
        self.webpage = webpage
        if semaphore:
            self.semaphore = semaphore
        else:
            self.semaphore = threading.BoundedSemaphore()

    def _can_fetch(self, url_):
        return self.robot.can_fetch(USER_AGENT, url_)

    def run(self):
        while self.has_content:
            start_time = time.time()
            with self.semaphore:
                try:
                    self._iter_once()
                except queue.Empty:
                    self.has_content = False
            try:
                time.sleep(
                    self.robot.crawl_delay + start_time - time.time())
            except ValueError:
                pass

    def _iter_once(self):
        link = self.links.get(timeout=1)
        if not self._can_fetch(link):
            return  # None, None

        try:
            webpage = self.webpage(
                url=link,
                base_url=self.base
            )
        except (urllib.error.HTTPError, UnicodeEncodeError):
            return  # None, None

        if webpage.followable:
            urlfetcher = WebpageLinks(url=link, base_url=self.base,
                                      html=webpage.html, download=False)
            self.base_url.add_links(urlfetcher, self.depth, self.base)
        webpage.content = None

        if webpage.archivable:
            try:
                webpage.store_content()
            except (TypeError, AttributeError):
                pass
        if VERBOSE:
            print(webpage.content)

        return  # link, webpage.content


class Crawler(object):

    def __init__(self, sitelist):
        self.semaphore = threading.BoundedSemaphore(MAX_THREADS)
        self.base_url = BaseUrl(sitelist)
        self.websites = []

    def run(self, iteration=1):
        while threading.activeCount() > 1 or self.base_url.base_queue.qsize() > 0:
            try:
                base = self.base_url.base_queue.get(timeout=5)
                thread = threading.Thread(target=self._one_run, args=(base,))
                thread.start()
            except queue.Empty:
                time.sleep(10)
            print("number of threads running:", threading.activeCount())
        if self.base_url.has_content and iteration < MAX_RUN_ITERATIONS:
            print("Run {i} is finished, starting new run after {s} seconds."
                  .format(i=iteration, s=RUN_WAIT_TIME))
            time.sleep(RUN_WAIT_TIME)
            self.run(iteration + 1)
        print("Finished")

    def _one_run(self, base):
        site, depth, historic_links, link_queue = base
        print(site, depth)
        self.websites.append(
            Website(
                base=site,
                link_queue=link_queue,
                historic_links=historic_links,
                webpage=WebpageText,
                base_url=self.base_url,
                depth=depth,
                semaphore=None
            )
        )
        self.websites[-1].run()



# TODO: Check if robotparser requires direct link to robots.txt
# TODO: ?skip urls in database? > Later
# TODO: add docstrings

if __name__ == "__main__":
    standalone_crawler = Crawler([BASE_URL])
    standalone_crawler.run()
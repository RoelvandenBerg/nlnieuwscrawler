__author__ = 'roelvdberg@gmail.com'

import copy
from datetime import datetime as dt
from gzip import GzipFile
import logging
import queue
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request as request
import urllib.robotparser as robotparser
from zipfile import ZipFile

from lxml import etree
from dateutil import parser as dtparser

import crawler.model as model
from crawler.settings import *

# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
printlogger = logging.StreamHandler()
printlogger.setLevel(logging.DEBUG)
logger.addHandler(printlogger)
logger.debug("NEW_CRAWL_RUN | " + dt.now().strftime('%H:%M | %d-%m-%Y |'))


# Regex taken from Django:
url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def url_validate(url):
    """
    Validate urls based on simple rules.
    - check for ms office types like .docx format
    - if the url has a three-letter-extension this should be htm, com, org, edu
      or gov

    :param url: url to check
    :return: True if url is valid
    """
    url_extensions = ["htm", "com", "org", "edu", "gov"]
    url = url.strip(r'/')
    try:
        docxtest = not (url[-1] == 'x' and url[-5] == ".")
    except IndexError:
        docxtest = True
    try:
        match = (url[-3] in url_extensions or not url[-4] == ".") \
                and docxtest \
                and not any(nofollowtxt in url for nofollowtxt in NOFOLLOW)
    except IndexError:
        return True
    return match


def url_validate_explicit(url):
    """
    Validate url based on regex and simple rules from url_validate.

    See url_validate and url_regex for the chosen rules.

    :param url: url to check
    :return: True if url is valid
    """
    match = bool(url_regex.search(url)) and url_validate(url)
    return match


class Head(object):
    """
    Contains head tags and values for a webpage after parse()

    :param tags: contains a dictionary of {tag: attribute name} and/or
        [attribute:] attribute name where attribute name is the name of the
        attribute in Head the found value is stored to.
    """
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
        """
        Initialize root with htmltree at head location.

        :param htmltree: lxml etree object of a webpage.
        """
        try:
            self.root = htmltree.xpath(self.location)[0]
        except IndexError:
            self.root = []

    def parse(self):
        """
        Parse root and search for elements in 'tags'.
        """
        for el in self.root:
            self.search(el)

    def find_name_value(self, tag_name_pair, element):
        """
        Try to find tag in element.

        :param tag_name_pair: selection of self.tags based on element-tag.
        :param element: element in root.
        :returns: name, value and boolean. Name is the name as is stored in
            self.tags, value, that what is found in the element, the  boolean
            indicates whether something is found.
        """
        for attribute, dictionary in tag_name_pair:
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
        """
        Searches a given element for tags and attributes given in self.tags.

        :param element: element in root.
        """
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
    """
    Return a string or "" based on the input.

    :param string: can be any object that can be turned into a string.
    :return: string or "" based on input
    """
    if string:
        return str(string)
    else:
        return ""


class Webpage(object):
    """
    Fetches content from an [url] and parses its contents on initialisation.

    This is a base class that can be inherited. Minimally one should overwrite
    the tag and name values. Children can set the following attributes:

    :param tag: sought tag(s) can be a list of strings or a string.
    :param name: the name (string) or list of names (this should correspond
        with self.tag, except if None is given) under which the tag should be
        stored in the Webpage object during parsing. If None is given, if it
        exists (not None) the attribute name (self.attr[i]) at the right index
        is used or else  the tag name (self.tag[i]) at the right index is taken
        as a name.
    :param attr: sought attribute(s) can be a list of strings or a string. The
        chosen form must correspond with self.tag.
    :param head: if True is given a metadata from the head will be stored in
        this attribute.
    :param robot_archive_options: website will not be archived if any of these
        is matched by attribute values from the robots metadata attribute in
        the head-section of a website.
    :param parser: HTML or XML lxml parser (etree.XML or etree.HTML).
    """
    tag = ""
    name = None
    attr = None
    head = True
    robot_archive_options = ("noarchive", "nosnippet", "noindex")
    parser = etree.HTML

    def __init__(self, url, html=None, base_url=None, *args, **kwargs):
        """
        Fetch all content from a site and parse it.

        Apart from the arguments described below, extra arguments and key-value
        pairs will be handed over to the fetch method and from that rest
        arguments are handed over to the parse method. View those methods for
        the extra possible parameters.

        :param url: the http-address of the website that is to be parsed.
        :param html: (optional) in case the content of a site is allready
            obtained, this can be given in html.
        :param base_url: (optional) the base url that belongs to this url.
        """
        if not isinstance(self.tag, list):
            self.tag = [self.tag]
            self.name = [self.name]
            self.attr = [self.attr]
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
        self.session = model.Session()

    def fetch(self, url=None, download=True, *args, **kwargs):
        """
        Fetches the content of a webpage, based on an url.

        Apart from the arguments described below, extra arguments and key-value
        pairs will be handed over to the parse method. View that method for
        the extra possible parameters.

        :param url: the url which content will be downloaded.
        :param download: default: True, if set to False, the url content will
            not be downloaded. The parse method will look at the html content
            given on initialization.
        """
        if download and not self.html:
            if url is None:
                url = self.url
            data, header = self.agent
            with request.urlopen(request.Request(url, headers=header)) \
                    as response:
                self.html = response.read()
        self.parse(*args, **kwargs)

    def parse(self, selector_string=None, selector_method_name="xpath"):
        """
        Parse html from self.html and store the retrieved content.

        Stores the tag that is sought from self.tag under the given name
        (self.name see class description of self.name for alternative naming).
        If selector parameters are given, parsing is handled in two steps.
        - First only relevant elements are selected from the parsed tree.
        - Last the resulting elements are parsed for tags and attributes.
        This way only sections of a page are parsed (e.g. only elements that
        fall within a <div> tag pair).

        :param selector_string: for example ".//a" for a hyperlink.
        :param selector_method_name: either 'xpath' or 'cssselect'
        """
        for i, tag in enumerate(self.tag):
            self.base_tree = self.parser(self.html)
            self.trees = [self.base_tree]
            if selector_string:
                self.trees = self._fetch_by_method(
                    selector_string, selector_method_name)
            parse_iterator = (self._get_attr(y, i) for x in self.trees
                              for y in x.iter(tag))
            content = self.parse_edit(parse_iterator)
            try:
                setattr(self, self.attr[i], content)
            except (TypeError, IndexError):
                try:
                    setattr(self, self.name[i], content)
                except (TypeError, IndexError):
                    setattr(self, tag, content)

    def parse_edit(self, iterator):
        """
        Optionally overwrite by a child class to adapt parsed elements.

        :param iterator: the iterator received from the parse method.
        :return: the result should be a list of strings.
        """
        return list(iterator)

    def _fetch_by_method(self, selector_string, selector_method_name):
        """
        Cuts up a self.trees into smaller bits by selector parameters.

        If selector parameters are given, parsing is handled in two steps.
        - First only relevant elements are selected from the parsed tree.
        - Last the resulting elements are parsed for tags and attributes.
        This way only sections of a page are parsed (e.g. only elements that
        fall within a <div> tag pair).

        :param selector_string: for example ".//a" for a hyperlink.
        :param selector_method_name: either 'xpath' or 'cssselect'
        :return: a list of lxml elementtrees.
        """
        return self.trees[0][selector_method_name](selector_string)

    def _get_attr(self, element, index):
        """
        Try to get attribute value or text from element.

        If this fails returns ""

        :param element: element to be parsed.
        :param index: tag index in self.attr
        :return: Returns attribute value or all underlying text of an element.
        """
        try:
            return element.attrib[self.attr[index]]
        except TypeError:
            return self._textwalk(element)
        except KeyError:
            return ""

    def _textwalk(self, element):
        """
        Get all text from element and all child elements recursively.

        :param element: element to be parsed.
        :return: all text from element and underlying children
        """
        children = [self._textwalk(x) + stringify(x.tail) for x in element]
        return stringify(element.text) + "".join(children)

    def find_in_head(self, attr):
        """
        Finds attribute in self.head.

        :param attr: head attribute name
        :return: head attribute or None when the attribute does not exist.
        """
        try:
            return getattr(self.head, attr)
        except:
            return None

    def store(self):
        """
        Overwrite in child classes to store content to other database tables.
        """
        self.store_page()

    def store_model(self, item):
        """
        Stores SQL Alchemy database item to database.

        :param item: SQL Alchemy database item
        """
        self.session.add(item)
        self.session.commit()

    @property
    def website_entry(self):
        """
        Website entry in database that belongs to this webpage.
        """
        return self.session.query(model.Website).filter_by(
            url=self.base_url).one()

    @property
    def webpage_entry(self):
        """
        Webpage entry in database that belongs to this webpage.
        """
        return self.session.query(model.Webpage).filter_by(
            url=self.url).one()

    def store_page(self):
        """
        Store parsed webpage to database.
        """
        datetimenow = dt.now()
        pub_timestring = self.find_in_head("time")
        if pub_timestring:
            published_time = dtparser.parse(pub_timestring, dayfirst=True)
        else:
            published_time = None
        head_item = model.Webpage(
            crawl_datetime=datetimenow,
            url=self.url,
            title=self.find_in_head("title"),
            description=self.find_in_head("description"),
            author=self.find_in_head("author"),
            published_time=published_time,
            expiration_time=self.find_in_head("expiration_time"),
            section=self.find_in_head("section"),
            tag=self.find_in_head("tag"),
            keywords=self.find_in_head("keywords"),
        )
        website = self.website_entry
        website.webpages.append(head_item)
        self.store_model(item=website)
        logger.debug('Webpage entry added: {}'.format(self.url))

    @property
    def agent(self):
        """
        Useragent data and headers for this crawler.
        """
        data = urllib.parse.urlencode(USER_AGENT_INFO)
        data = data.encode('utf-8')
        headers = {'User-Agent': USER_AGENT}
        return data, headers

    @property
    def head_robots(self):
        """
        Text from head.robots
        """
        try:
            return self.head.robots.lower()
        except AttributeError:
            return ""

    @property
    def followable(self):
        """
        Boolean: Head information states if a page is followable
        """
        return not "nofollow" in self.head_robots

    @property
    def archivable(self):
        """
        Boolean: Head information states if a page is archivable.
        """
        return not any(robotsetting in self.head_robots for robotsetting in
                       self.robot_archive_options)


class WebpageText(Webpage):
    """
    Fetches content from webpage by url and returns its text.
    """
    tag = "p"
    name = "text"

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)

    def store(self):
        """
        Stores paragraphs and header metadata to database.
        """
        text = [x for x in [txt.strip(' \t\n\r')
                            for txt in self.text] if x != ""]
        logger.debug("storing {} paragraphs".format(len(text)))
        self.store_page()
        webpage = self.webpage_entry
        for paragraph in text:
            new_item = model.Paragraph(
                paragraph=paragraph,
            )
            webpage.paragraphs.append(new_item)
        self.store_model(item=webpage)
        logger.debug('Stored webpagetext for: ' + self.url)


class WebpageLinks(Webpage):
    """
    Fetches content from webpage by url and returns its hyperlinks.
    """
    tag = "a"
    attr = "href"
    name = "links"

    def parse_edit(self, iterator):
        """
        When parsing, validate url.

        :param iterator: list of urls to validate
        :return: list of valid urls
        """
        return [link for link in iterator if url_validate(link)]


class SitemapMixin(object):
    """
    Adds a list of visited urls, addition and iteration functionality.
    """
    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.visited = []

    def __add__(self, other):
        sum_ = copy.deepcopy(self)
        sum_.visited += other.visited
        sum_.links += other.links
        return sum_

    def __iadd__(self, other):
        return self.__add__(other)

    def __iter__(self):
        """
        Iterate over self.links.

        Only yield a sitmap url when it has not yet been visited. Only add
        sitemaps when full sitemaps are encountered.
        """
        for link in self.links:
            if link[-3] == "xml":
                higher_sitemap = XMLSitemap(link)
                self += higher_sitemap
                self.visited.append(link)
            elif link not in self.visited:
                yield link
                self.visited.append(link)


class HTMLSitemap(SitemapMixin, WebpageLinks):
    """
    Parses HTML sitemaps.
    """
    pass


class XMLSitemap(SitemapMixin, Webpage):
    """
    Parses XML sitemaps.
    """
    tag = "loc"
    name = "links"
    parser = etree.XML


class ZipSitemap(XMLSitemap):
    """
    Parses zipped (.zip) XML sitmaps.
    """
    zip_method = ZipFile

    def extract_zip(self, input_zip):
        """
        Extract zipfile with multiple files to memory.
        """
        zip_file = self.zip_method(input_zip)
        return {name: zip_file.read(name) for name in zip_file.namelist()}

    def fetch(self, url=None, download=None, *args, **kwargs):
        """
        fetch is re(- and over)written to add unzipping.
        """
        if url is None:
            url = self.url
        data, header = self.agent
        with request.urlopen(request.Request(url, headers=header)) \
                as response:
            zipped = response.read()
            unzipped_xmls = self.extract_zip(zipped)
        self.old = ZipSitemap("")
        for key in unzipped_xmls.keys():
            self.html = unzipped_xmls[key]
            self_old = copy.deepcopy(self)
            self.parse(*args, **kwargs)
            self += self_old


class GunZipSitemap(ZipSitemap):
    """
    Parses gunzipped (.gz) XML sitmaps.
    """
    zip_method = GzipFile


class RobotTxt(robotparser.RobotFileParser):
    """
    Extention of robotparser, adds sitemap functionality, mainly a copy.

    Additions:
    - sitemaps
    - logging
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

        self.modified()
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
                    elif sitemap_url.endswith('.zip'):
                        sitemap_class = ZipSitemap
                    elif sitemap_url.endswith('.gz'):
                        sitemap_class = GunZipSitemap
                    elif sitemap_url == '/sitemapindex/':
                        sitemap_url = self.url[:-10] + "sitemapindex"
                        sitemap_class = HTMLSitemap
                    else:
                        sitemap_class = HTMLSitemap
                    if self.sitemap:
                        logger.debug(
                            "    ADDING SITEMAP {}".format(sitemap_url))
                        self.sitemap += sitemap_class(sitemap_url)
                    else:
                        logger.debug("LOADING SITEMAP {}".format(sitemap_url))
                        self.sitemap = sitemap_class(sitemap_url)
                elif line[0].lower().startswith('crawl-delay'):
                    new_delay = float(line[1])
                    if self.crawl_delay < new_delay:
                        self.crawl_delay = new_delay
        if state == 2:
            self._add_entry(entry)

    def can_fetch(self, useragent, url):
        """using the parsed robots.txt decide if useragent can fetch url"""
        if self.disallow_all:
            logger.debug('ROBOTPARSER CAN_FETCH ERROR: dissalow_all')
            return False
        if self.allow_all:
            return True
        # Until the robots.txt file has been read or found not
        # to exist, we must assume that no url is allowable.
        # This prevents false positives when a user erronenously
        # calls can_fetch() before calling read().
        if not self.last_checked:
            logger.debug('ROBOTPARSER CAN_FETCH ERROR: last_checked')
            return False
        # search for given user agent matches
        # the first match counts
        parsed_url = urllib.parse.urlparse(urllib.parse.unquote(url))
        url = urllib.parse.urlunparse(('', '', parsed_url.path,
                                       parsed_url.params, parsed_url.query,
                                       parsed_url.fragment))
        url = urllib.parse.quote(url)
        if not url:
            url = "/"
        for entry in self.entries:
            if entry.applies_to(useragent):
                logger.debug(
                    'ROBOTPARSER CAN_FETCH ERROR: entry.allowance(url)')
                return entry.allowance(url)
        # try the default entry last
        if self.default_entry:
            logger.debug(
                'ROBOTPARSER CAN_FETCH ERROR: default_entry.allowance(url)')
            return self.default_entry.allowance(url)
        # agent not found ==> access granted
        return True


class EmptyWebpage(object):
    """
    Empty webpage class that can serve as a dummy class for Crawler.
    """

    def __init__(self, *args, **kwargs):
        pass


class BaseUrl(list):
    """
    A list of baseurls that can be crawled.

    The baseurl is the part of the url before the first / (apart from the two
    slashes after http(s):). BaseUrl is used to administrate which urls can be
    crawled, which still need to be crawled. Base urls are ordered
    according to their crawl depth. Where the first base urls have a depth of 0
    and urls found within a webpage that do not match that webpages' base url
    have an increased depth (by 1). Thus a BaseUrl has the following form:

    [[BASE_URL, ...]n=0, [..., ...]n=1, [..., ...]n=2, ..., []n=CRAWL_DEPTH]

    The CRAWL_DEPTH can be set in the settings file.

    :param lock: The BaseUrl has a lock that is used during threading. This way
        multiple threads can handle the BaseUrl.
    :param base_queue: a queue with websites that have not yet been crawled.

    Within a BaseUrl Each base url is stored as a list of parameters:
    [0]: the base url string
    [1]: historic_links: a list of all links that have been found (within each
        thread that crawls for urls).
    [2]: link_queue: a queue of all links that still need to be crawled.
    """
    base_regex = re.compile(r"^(?:http)s?://[\w\-_\.]+", re.IGNORECASE)

    def __init__(self, base):
        """
        :param base: either a string with a base url or a list of strings with
            base urls.
        """
        super().__init__()
        self.session = model.Session()
        self += [[] for _ in range(CRAWL_DEPTH + 1)]
        self.lock = threading.RLock()
        self.base_queue = queue.Queue()
        if isinstance(base, str):
            base = [base]
        for base_url in base:
            self.append(base_url, 0)

    def store(self, url):
        """
        Stores url to website table in database.
        :param url: website base url
        """
        new_item = model.Website(url=url)
        self.session.add(new_item)
        self.session.commit()

    def add(self, url, current_depth):
        """
        Adds a url to self.

        :param url: regular url to be added.
        :param current_depth: depth at which the url has been harvested.
        """
        with self.lock:
            # iterate over base urls at each depth in self to check if url is
            # in one of the base urls
            for i, link_bundle in enumerate(self):
                for base, link_history, link_queue in link_bundle:
                    # test if url contains base and hasn't been added before
                    if url.startswith(base):
                        if not url in link_history:
                            # link hasn been added before, so store it
                            link_history.append(url)
                            link_queue.put(url)
                        return
            # link has not been matched to any of the base urls, so append it
            # as a base url.
            current_depth += 1
            logger.debug(
                'adding new site @depth {} : {}'.format(current_depth,
                                                        url))
            self.append(url, current_depth)

    def append(self, url, depth):
        """
        Extract an urls base, and append the url and the base to self.

        :param url: a url to be added with its base.
        :param depth: crawl depth the url has to be added to.
        """
        if depth > CRAWL_DEPTH:
            return
        with self.lock:
            base = self.base_regex.findall(url)[0] + "/"
            if base not in self[depth] or url_validate_explicit(url):
                link_queue = queue.Queue()
                link_queue.put(url)
                historic_links = [url]
                # base urls are added to the crawl queue only if set in
                # settings:
                if ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE:
                    link_queue.put(base)
                    historic_links.append(base)
                self[depth].append((url, historic_links, link_queue))
                self.base_queue.put((base, depth, historic_links, link_queue))
                self.store(base)

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
                if not url_.startswith("http"):
                    url_ = urllib.parse.urljoin(base_url, url_)
                if not url_validate_explicit(url_):
                    continue
                self.add(url_, depth)
        except AttributeError:
            pass

    def __str__(self):
        return "BaseUrl with at depth 0: " + \
               ", ".join(self.base)

    def __repr__(self):
        return "\n".join(
            [
                "\nDEPTH {}:\n".format(str(i)) +
                "\n".join(
                    ["    - qsize: {} - {}\n".format(base[2].qsize(), base[0])
                     + "\n".join(
                        ["        o {} ".format(url) for url in base[1]]
                    ) for base in layer]
                ) for i, layer in enumerate(self)
            ]
        )

    @property
    def base(self):
        """
        List of base urls at lowest depth.
        """
        return [x[0] for x in self[0]]

    @property
    def has_content(self):
        """
        Indicates whether any of the base urls still has links in their queue.
        """
        try:
            return any(any(url[2].qsize() > 0 for url in urls_at_one_depth)
                       for urls_at_one_depth in self)
        except IndexError:
            return False


class Website(object):
    """
    Website crawler that crawls all pages in a website.
    """

    def __init__(self, base, link_queue, historic_links, webpage=WebpageText,
                 base_url=None, depth=0):
        """
        :param base: base url string .
        :param link_queue: queue from base url.
        :param historic_links: historic links from base url.
        :param webpage: WebPage class or one of its children.
        :param base_url: BaseUrl object that at least contains this website.
        :param depth: crawl depth of this website.
        """
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

    def _can_fetch(self, url_):
        """
        Tests if robots.txt accepts url_ as crawlable.
        :param url_: url to be tested
        :return: True or False
        """
        return self.robot.can_fetch(USER_AGENT, url_)

    def run(self):
        """Runs a website crawler."""
        while self.has_content:
            start_time = time.time()
            try:
                self._run_once()
            except queue.Empty:
                self.has_content = False
            try:
                time.sleep(
                    self.robot.crawl_delay + start_time - time.time())
            except ValueError:
                logger.debug(
                    "No crawl delay was found with {} !".format(self.base))
                time.sleep(
                    CRAWL_DELAY + start_time - time.time())

    def _run_once(self):
        """Runs one webpage of a website crawler."""
        logger.debug('self.iter_once: {url}'.format(url=str(self.base)))
        link = self.links.get(timeout=1)
        if not self._can_fetch(link):
            logger.debug('{} CANNOT BE FETCHED.'.format(link))
            return
        try:
            webpage = self.webpage(
                url=link,
                base_url=self.base
            )
        except (urllib.error.HTTPError, UnicodeEncodeError):
            logger.debug('HTTPERROR: {}'.format(link))
            return
        if webpage.followable:
            urlfetcher = WebpageLinks(url=link, base_url=self.base,
                                      html=webpage.html, download=False)
            self.base_url.add_links(urlfetcher, self.depth, self.base)
        else:
            logger.debug('WEBSITE NOT FOLLOWABLE: {}'.format(link))
        webpage.content = None

        if webpage.archivable:
            try:
                webpage.store()
            except (TypeError, AttributeError):
                logger.debug(
                    'STORE CONTENT NOT WORKING FOR SITE: {}'.format(link))
        else:
            logger.warn('WEBSITE NOT ARCHIVABLE: {}'.format(link))
        if VERBOSE:
            logger.debug(webpage.content)


class Crawler(object):
    """
    Crawler that crawls sites based on a sitelist.
    Results are stored in a database defined in model.
    """
    def __init__(self, sitelist):
        """
        :param sitelist: a list of sites to be crawled.
        """
        self.base_url = BaseUrl(sitelist)
        self.websites = []

    def run(self):
        """Run crawler"""
        number_of_website_threads = 2
        # Run while there is still active website-threads left.
        while number_of_website_threads > 0:
            number_of_website_threads = threading.activeCount() - 1
            # Run as much threads as MAX_THREADS (from settings) sets.
            while number_of_website_threads < MAX_THREADS:
                # start a new website thread:
                try:
                    base = self.base_url.base_queue.get(timeout=5)
                    thread = threading.Thread(
                        target=self._website_worker,
                        args=(base,)
                    )
                    thread.start()
                except queue.Empty:
                    pass
                logger.debug(
                    "Number of threads with websites running: {}".format(
                        number_of_website_threads))
        logger.debug("Finished")
        logger.debug(repr(self.base_url))

    def _website_worker(self, base):
        """
        Worker that crawls one website.
        :param base: base instance from base_queue from a BaseUrl object.
        """
        site, depth, historic_links, link_queue = base
        logger.debug("RUN FOR {} DEPTH: {}".format(site, depth))
        website = Website(
            base=site,
            link_queue=link_queue,
            historic_links=historic_links,
            webpage=WebpageText,
            base_url=self.base_url,
            depth=depth,
        )
        website.run()
        self.websites.append(website)


if __name__ == "__main__":
    standalone_crawler = Crawler([BASE_URL])
    standalone_crawler.run()

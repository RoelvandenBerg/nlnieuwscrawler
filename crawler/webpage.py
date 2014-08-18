# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
import dateutil.parser as dtparser
import logging
import threading
import urllib.parse
import urllib.request as request

from lxml import etree
from sqlalchemy.orm.exc import NoResultFound

import model
from settings import USER_AGENT_INFO, USER_AGENT
import validate

# setup logger
logger = logging.getLogger(__name__)
printlogger = logging.StreamHandler()
printlogger.setLevel(logging.DEBUG)
logger.addHandler(printlogger)


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
                "revisit-after": "revisit_after",
                "robots": "robots"
            }),
            ("property", {
                "og:description": "description",
                "og:title": "title",
                "article:published_time": "published_time",
                "article:modified_time": "modified_time",
                "article:expiration_time": "expiration_time",
                "article:author": "author",
                "article:section": "section",
                "article:tag": "article_tag"
            }),
        )
    }

    def __init__(self, htmltree, html):
        """
        Initialize root with htmltree at head location.

        :param htmltree: lxml etree object of a webpage.
        """
        try:
            self.root = htmltree.xpath(self.location)[0]
        except IndexError:
            self.root = []
        self.htmltree = htmltree
        self.html = html

    def parse(self):
        """
        Parse root and search for elements in 'tags'.
        """
        for el in self.root:
            self.search(el)
        if not 'time' in self.__dict__:
            try:
                self.parse_time()
            except:
                pass

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


class WebpageRaw(object):
    robot_archive_options = ["noarchive", "nosnippet", "noindex"]
    head = True
    parser = etree.HTML

    def __init__(self, url, html=None, base_url=None, database_lock=None,
                 *args, **kwargs):
        """
        Fetch all content from a site and store it in text format.

        Apart from the arguments described below, extra arguments and key-value
        pairs will be handed over to the fetch method and from that rest
        arguments are handed over to the parse method. View those methods for
        the extra possible parameters.

        :param url: the http-address of the website that is to be parsed.
        :param base_url: (optional) the base url that belongs to this url.
        """
        if not database_lock:
            self.database_lock = threading.RLock()
        else:
            self.database_lock = database_lock
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = url
        self.html = html
        self.url = url
        self.session = model.Session()
        self.fetch(*args, **kwargs)
        if self.head:
            self.head = Head(self.base_tree, self.html)
            self.head.parse()

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

    def parse(self, *args, **kwargs):
        self.base_tree = self.parser(self.html)

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
        with self.database_lock:
            self.session.add(item)
            self.session.commit()

    @property
    def website_entry(self):
        """
        Website entry in database that belongs to this webpage.
        """
        with self.database_lock:
            return self.session.query(model.Website).filter_by(
                url=self.base_url).one()

    @property
    def webpage_entry(self):
        """
        Webpage entry in database that belongs to this webpage.
        """
        with self.database_lock:
            return self.session.query(model.Webpage).filter_by(
                url=self.url).all()

    @property
    def last_webpage_entry(self):
        return self.webpage_entry[-1]

    @property
    def webpage_created(self):
        try:
            return self.webpage_entry[0].crawl_created
        except (AttributeError, IndexError, NoResultFound):
            return dt.now()

    def store_page(self):
        """
        Store parsed webpage to database.
        """
        with self.database_lock:
            datetimenow = dt.now()
            times = {"published_time": None, "modified_time": None,
                     "expiration_time": None}
            for time in times.keys():
                timestr = self.find_in_head(time)
                if timestr:
                    times[time] = dtparser.parse(timestr, dayfirst=True)
            head_item = model.Webpage(
                content=self.html,
                crawl_created=self.webpage_created,
                crawl_modified=datetimenow,
                url=self.url,
                revisit=self.find_in_head("revisit_after"),
                published_time=times['published_time'],
                modified_time=times["modified_time"],
                expiration_time=times["expiration_time"],
                title=self.find_in_head("title"),
                description=self.find_in_head("description"),
                author=self.find_in_head("author"),
                section=self.find_in_head("section"),
                tag=self.find_in_head("article_tag"),
                keywords=self.find_in_head("keywords")
            )
            website = self.website_entry
            website.modified = datetimenow
            website.webpages.append(head_item)
            self.store_model(item=website)
            logger.debug('Webpage entry added: {}'.format(self.url))


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


class Webpage(WebpageRaw):
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
    :param split_content: if True saves content as attributes to self else
        content is only stored as 'content'.
    :param head: if True is given a metadata from the head will be stored in
        this attribute.
    :param robot_archive_options: website will not be archived if any of these
        is matched by attribute values from the robots metadata attribute in
        the head-section of a website.
    :param parser: HTML or XML lxml parser (etree.XML or etree.HTML).
    """
    tag = ""
    name = []
    attr = []
    split_content = True
    one_tag = False

    def __init__(self, url, html=None, base_url=None, database_lock=None,
                 *args, **kwargs):
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
        if not isinstance(self.name, list):
            self.name = [self.name]
        if not isinstance(self.attr, list):
            self.attr = [self.attr]
            self.one_tag = True
        super().__init__(url, html, base_url, database_lock, *args,
                         **kwargs)

    def parse(self, selector_string=None, selector_method_name="xpath"):
        """
        Parse html from self.html and store the retrieved content.

        If selector parameters are given, parsing is handled in two steps.
        - First only relevant elements are selected from the parsed tree.
        - Last the resulting elements are parsed for tags and attributes.
        This way only sections of a page are parsed (e.g. only elements that
        fall within a <div> tag pair).

        :param selector_string: for example ".//a" for a hyperlink.
        :param selector_method_name: either 'xpath' or 'cssselect'
        """
        super().parse()
        self.trees = [self.base_tree]
        if selector_string:
            self.trees = self._fetch_by_method(
                selector_string, selector_method_name)
        for i, t in enumerate(self.tag):
            content = [self._get_attr(y, i) for x in self.trees for y
                              in x.iter() if y.tag == t]
            self._set_content(i, t, content)
        self.parse_edit()

    def _set_content(self, i, tag, content):
        """
        Stores the tag content under the given name (self.name; see class
        description of self.name for alternative naming).
        """
        try:
            setattr(self, self.name[i], content)
        except (TypeError, IndexError):
            try:
                setattr(self, self.attr[i], content)
            except (TypeError, IndexError):
                setattr(self, tag, content)

    def parse_edit(self):
        """
        Optionally overwrite by a child class to adapt parsed elements.

        :param iterator: the iterator received from the parse method.
        :return: the result should be a list of strings.
        """
        pass

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

    def _get_attr(self, element, i):
        """
        Try to get attribute value or text from element.

        If this fails returns ""

        :param element: element to be parsed.
        :param index: tag index in self.attr
        :return: Returns attribute value or all underlying text of an element.
        """
        try:
            return element.attrib[self.attr[i]]
        except (TypeError, IndexError):
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


class Empty(WebpageRaw):
    """
    Empty webpage class that can serve as a dummy class for Crawler.
    Using this, the crawler will work in its purest form: only harvesting
    hyperlinks.
    """

    def parse(self, selector_string=None, selector_method_name="xpath"):
        self.base_tree = self.parser(self.html)


class Text(Webpage):
    """
    Fetches content from webpage by url and returns its text.
    """
    tag = "p"
    name = "text"

    def store(self):
        """
        Stores paragraphs and header metadata to database.
        """
        with self.database_lock:
            text = [x for x in [txt.strip(' \t\n\r')
                                for txt in self.text] if x != ""]
            logger.debug("storing {} paragraphs".format(len(text)))
            self.store_page()
            webpage = self.last_webpage_entry
            for paragraph in text:
                new_item = model.Paragraph(
                    paragraph=paragraph,
                )
                webpage.paragraphs.append(new_item)
            self.store_model(item=webpage)
            logger.debug('Stored webpagetext for: ' + self.url)


class HeadingText(Webpage):
    paragraph_tags = ['p', 'li']
    heading_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
    tag = ['p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']
    name = ['p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']
    split_content = False

    def store(self):
        """
        Stores paragraphs, headings and header metadata to database.
        """
        with self.database_lock:
            logger.debug(
                "Storing {} paragraphs and headings for {}".format(
                    sum([len(getattr(self, x, [])) for x in
                        self.paragraph_tags]),
                    self.url
                )
            )
            self.store_page()
            previous_headings = {h: None for h in self.heading_tags}
            webpage = self.last_webpage_entry
            first_heading = True
            heading = None
            for tag in self.paragraph_tags:
                try:
                    elements = getattr(self, tag)
                except AttributeError:
                    continue
                for item in elements:
                    item = item.strip(' \t\n\r')
                    if item == "":
                        continue
                    if tag in self.paragraph_tags and not first_heading:
                        new_paragraph = model.Paragraph(
                            paragraph=item,
                        )
                        heading.paragraphs.append(new_paragraph)
                        webpage.paragraphs.append(new_paragraph)
                    else:
                        if not first_heading:
                            webpage.headings.append(heading)
                        else:
                            first_heading = False
                        previous_headings[tag] = item
                        heading = model.Heading(
                            h1=previous_headings['h1'],
                            h2=previous_headings['h2'],
                            h3=previous_headings['h3'],
                            h4=previous_headings['h4'],
                            h5=previous_headings['h5'],
                            h6=previous_headings['h6']
                        )
            self.store_model(item=webpage)
            logger.debug('Stored paragraphs and headings for: ' + self.url)


class Links(Webpage):
    """
    Fetches content from webpage by url and returns its hyperlinks.
    """
    tag = ["a", "a"]
    attr = ["href", "rel"]
    name = ["links", "robots"]

    def parse_edit(self):
        """
        When parsing, validate url. And check links for nofollow.
        """
        robot_nofollow = self.robot_archive_options + ['nofollow']
        self.links = [link for i, link in enumerate(self.links) if validate.url(
            link) and not self.robots[i] in robot_nofollow]

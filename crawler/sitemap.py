# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import copy
from gzip import GzipFile
import urllib.request as request
import logging
from io import BytesIO
from zipfile import ZipFile

import lxml.etree as etree

try:
    import webpage
    import validate
except ImportError:
    import crawler.webpage as webpage
    import crawler.validate as validate


# setup logger
logger = logging.getLogger(__name__)
printlogger = logging.StreamHandler()
printlogger.setLevel(logging.DEBUG)
logger.addHandler(printlogger)


class Sitemap(object):

    def __init__(self, url, base_url, html=None, first=True):
        if not url.startswith('http'):
            if not url.startswith('www'):
                new_url = base_url + '/' + url.strip('/')
                self.url = new_url
            else:
                self.url = 'http://' + url.strip('/')
        else:
            self.url = url
        self.base_url = base_url
        self.first = first
        self.html = html
        self.sitemap = False
        logger.debug('SITEMAP INITIALIZED: ' + self.url)
        self.choose()

    def choose(self):
        print('choose', self.base_url)
        # determine what type of sitemap it entails.
        if self.url.endswith('.gz'):
            logger.debug('GunZip sitemap chosen: ' + self.url)
            self.klass = GunZip
        elif self.url.endswith('.xml') or (self.url.startswith(
                'google') and self.url.endswith('map')):
            logger.debug('XML sitemap chosen: ' + self.url)
            self.klass = XmlSitemapIndex
        elif self.url.endswith('.txt'):
            logger.debug('TXT sitemap chosen: ' + self.url)
            self.klass = Txt
        elif self.url.endswith('.zip'):
            logger.debug('Zip sitemap chosen: ' + self.url)
            self.klass = Zip
        elif self.url == '/sitemapindex/':
            logger.debug('XML sitemap chosen: ' + self.url)
            self.url = self.base_url[:-10] + "sitemapindex"
            self.klass = XmlSitemapIndex
        else:
            logger.debug('unknown sitemaptype, switching to XML sitemap Index: '
                         + self.url)
            self.klass = XmlSitemapIndex

        print(self.klass)
        try:
            if self.sitemap:
                logger.debug(
                    "SITEMAP: added {}".format(self.url))
                self.sitemap += self.klass(self.url, html=self.html)
            else:
                logger.debug(
                    "SITEMAP: loading {}".format(self.url))
                self.sitemap = self.klass(self.url, html=self.html,
                                          base_url=self.base_url)
                print('sitemap loaded.', self.url)
        except (AttributeError, TypeError, ValueError):
            logger.debug(
                "SITEMAP: LOADING FAILED {}".format(self.url))
            raise Exception('sitemap_url ' + self.url)


class SitemapMixin(object):
    """
    Adds a list of visited urls, addition and iteration functionality.
    """
    xml = False
    head = False

    def __init__(self, url, html=None, base_url=None):
        logger.debug('INIT SITEMAPMIXIN ' + url)
        self.visited = []
        self.links = []
        self.modified_time = False
        self.publication_date = False
        self.revisit = False
        self.description = False
        super().__init__(url, html=html, base_url=base_url)

    def _attr_len(self, sitemap):
        attributes = ["links", "modified_time", "revisit", 'publication_date',
            'title', 'description']
        attr_len = [(a, len(a)) if getattr(sitemap, a, False) else (a, -1) for
                    a in attributes]
        max_ = max(attr_len, key=lambda x: x[1])[1]
        return attr_len, max_, all(x[0] == max_ or x[0] == -1 for x in attr_len)

    def _diff(self, one, other):
        change = [y[0] not in [x[0] for x in one] for y in other]
        changes = [other[i] for i, x in enumerate(change) if x]
        return any(change), changes

    def _update(self, p_object, length, one_attr, other_attr, change_one):
        change, changes = self._diff(one_attr, other_attr)
        if change_one and change:
            for change in changes:
                setattr(p_object, change, [''] * length)

    def __add__(self, other):
        print('adding __add__ sitemaps')
        print(self.links, other.links)
        self.visited += other.visited
        self.links += other.links
        self_attr, max_self, change_self = self._attr_len(self)
        other_attr, max_other, change_other = self._attr_len(other)
        self._update(self, max_other, self_attr, other_attr, change_self)
        self._update(other, max_self, other_attr, self_attr, change_other)
        print('added __add__ sitemaps')
        print(self.url, self.links)
        return self

    def __iadd__(self, other):
        return self.__add__(other)

    def __iter__(self):
        """
        Iterate over self.links.

        Only yield a sitemap url when it has not yet been visited. Only add
        sitemaps when full sitemaps are encountered.
        """
        print('__iter__', self.url, self.links)
        for link in self.links:
            if link not in self.visited:
                print('SITEMAP YIELDING ' + link)
                yield link
                self.visited.append(link)


class Html(SitemapMixin, webpage.Links):
    """
    Parses HTML sitemaps.
    """
    pass


class Rss(SitemapMixin, webpage.Webpage):
    selector_string='.//item'
    tag = ["link", "lastBuildDate", "pubDate", "title", "description"]
    name = ["links", "modified_time", 'publication_date', 'title',
            'description']
    parser = etree.XML
    xml = True

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.modified_time = [
            mod_time if mod_time != '' else self.publication_date[i]
            for i, mod_time in enumerate(self.modified_time)
        ]


class XmlSitemap(SitemapMixin, webpage.Webpage):
    """
    Parses XML sitemapindexes.
    """
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]
    parser = etree.HTML
    xml = True
    selector_string='.//sitemapindex'
    next = None

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        logger.debug('SITEMAP: loading XML' + base_url + ' WITH NUMBER OF '
                                                         'LINKS: ' + str(len(
            self.links)))
        if not len(self.links):
            logger.debug('SITEMAP: loading different XML: ' + base_url +
                         ' with type: ' + str(self.next))
            self += self.next(url, html=self.html, base_url=base_url)

    def parse_edit(self):
        new_links = []
        new_modified_time = []
        new_revisit = []
        for link in self.links:
            xml = XmlUrlset(link, base_url=self.base_url)
            new_links += xml.get('links', [])
            new_revisit += xml.get('revisit', [])
            new_modified_time += xml.get('modified_time', [])
        self.links = new_links
        if new_modified_time:
            self.modified_time = new_modified_time
        elif getattr(self, 'modified_time', False):
            del self.modified_time
        if new_revisit:
            self.revisit = new_revisit
        elif getattr(self, 'revisit', False):
            del self.revisit


class XmlUrlset(XmlSitemap):
    """
    Parses XML sitemaps.
    """
    selector_string='.//urlset'
    next = Rss


class XmlSitemapIndex(XmlSitemap):
    """
    Parses XML sitemapindexes.
    """
    next = XmlUrlset


class Txt(webpage.WebpageRaw):
    """
    Parses Txt sitemaps.
    """
    head = False
    visited = []

    def parse(self, *args, **kwargs):
        self.links = [y for x in self.html.split('\n') for y in x.split('\r')
                     if validate.url_explicit(y)]


class GunZip(SitemapMixin, webpage.Webpage):
    """
    Parses gunzipped (.gz) XML sitmaps.
    """

    def fetch(self, url=None, download=None, *args, **kwargs):
        if not url:
            url = self.url
        data, header = self.agent
        with request.urlopen(request.Request(url, headers=header)) \
                as response:
            self.html = response.read()
        self.parse()

    def parse(self):
        fileobj = BytesIO(self.html)
        zipped = GzipFile(fileobj=fileobj, mode='rb')
        unzipped_xml = zipped.read()
        print('gzip')
        sitemap = Sitemap(url.strip('.gz'), html=unzipped_xml,
                          base_url=self.base_url)
        print('adding gz sitemap')
        self += sitemap.sitemap
        print('added gz sitemap')
        self.xml = sitemap.sitemap.xml
        print('xml set', self.url)

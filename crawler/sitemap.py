# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import copy
from gzip import GzipFile
import urllib.request as request
import logging
from io import BytesIO
from zipfile import ZipFile

import lxml.etree as etree

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
                self.url = 'http://' + url
        else:
            self.url = url
        self.base_url = base_url
        self.first = first
        self.html = html
        self.sitemap = False
        self.choose()

    def choose(self):
        # determine what type of sitemap it entails.
        if self.url.endswith('.xml') or (self.url.startswith(
                'google') and self.url.endswith('map')):
            logger.debug('XML sitemap chosen: ' + self.url)
            self.klass = XmlSitemapIndex
        elif self.url.endswith('.gz'):
            logger.debug('GunZip sitemap chosen: ' + self.url)
            self.klass = GunZip
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
            logger.debug('unknown sitemaptype, switching to XML: '
                         + self.url)
            self.klass = XmlSitemapIndex

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
        except (AttributeError, TypeError, ValueError):
            logger.debug(
                "SITEMAP: LOADING FAILED {}".format(self.url))
            raise Exception('sitemap_url')


class SitemapMixin(object):
    """
    Adds a list of visited urls, addition and iteration functionality.
    """
    xml = False
    head = False

    def __init__(self, url, html=None, base_url=None):
        self.visited = []
        self.links = []
        super().__init__(url, html=html, base_url=base_url)

    def __add__(self, other):
        self.visited += other.visited
        self.links += other.links
        return self

    def __iadd__(self, other):
        return self.__add__(other)

    def __iter__(self):
        """
        Iterate over self.links.

        Only yield a sitemap url when it has not yet been visited. Only add
        sitemaps when full sitemaps are encountered.
        """
        for link in self.links:
            if link not in self.visited:
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
            logger.debug('SITEMAP: loading different XML: ' + base_url + 'with '
                                                                    'type: ' + str(next))
            self += self.next(url, html=self.html, base_url=base_url)


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


class Zip(SitemapMixin, webpage.Webpage):
    """
    Parses zipped (.zip) XML sitmaps.
    """

    def extract_zip(self, input_zip):
        """
        Extract zipfile with multiple files to memory.
        """
        zip_file = ZipFile(BytesIO(input_zip), mode='rb')
        x = {name: zip_file.read(name) for name in zip_file.namelist()}
        return x

    def fetch(self, url=None, download=None, *args, **kwargs):
        """
        fetch is re(- and over)written to add unzipping.
        """
        if url is None:
            url = self.url
        data, header = self.agent
        with request.urlopen(request.Request(url, headers=header)) \
                as response:
            zipped = BytesIO(response.read())
            unzipped_xmls = self.extract_zip(zipped)
        self.old = copy.deepcopy(self)
        for key in unzipped_xmls.keys():
            self.html = unzipped_xmls[key]
            self_old = copy.deepcopy(self)
            self.parse(*args, **kwargs)
            self += self_old


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
            fileobj = BytesIO(response.read())
            zipped = GzipFile(fileobj=fileobj, mode='rb')
        unzipped_xml = zipped.read()
        sitemap = Sitemap(url.strip('.gz'), html=unzipped_xml,
                          base_url=self.base_url)
        self += sitemap.sitemap
        self.xml = sitemap.sitemap.xml

# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

from gzip import GzipFile
import os
import urllib.error

import lxml.etree as etree

try:
    from base import logger_setup
    import webpage
    import validate
except ImportError:
    from crawler.base import logger_setup
    import crawler.webpage as webpage
    import crawler.validate as validate


logger = logger_setup(__name__)


class Sitemap(object):

    def __init__(self, urls, base, html=None, first=True):
        self.urls = []
        if isinstance(urls, str):
            urls = [urls]
        for url in urls:
            if not url.startswith('http'):
                if not url.startswith('www'):
                    new_url = base + '/' + url.strip('/')
                    self.urls.append(new_url)
                else:
                    self.urls.append('http://' + url.strip('/'))
            else:
                self.urls.append(url)
        self.base = base
        self.first = first
        self.html = html
        self.iterator = iter(self._iterator())
        self.iterable = True
        logger.debug('SITEMAP INITIALIZED: ' + self.base)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterator)

    def _iterator(self):
        while len(self.urls):
            url = self.urls.pop()
            print(url)
            sitemap = self.choose(url)
            print(sitemap)
            for content in sitemap:
                yield content
        self.iterable = False
        raise StopIteration
        yield

    def append(self, url):
        self.urls.append(url)
        if not self.iterable:
            self.iterator = iter(self._iterator())
            self.iterable = True

    def choose(self, url):
        # determine what type of sitemap it entails.
        if url.endswith('.gz'):
            logger.debug('GunZip sitemap chosen: ' + url)
            klass = GunZip
        elif url.endswith('.xml') or (url.startswith('google') and
                                          url.endswith('map')):
            logger.debug('XML sitemap chosen: ' + url)
            klass = XmlSitemapIndex
        elif url.endswith('.txt'):
            logger.debug('TXT sitemap chosen: ' + url)
            klass = Txt
        elif url == '/sitemapindex/':
            logger.debug('XML sitemap chosen: ' + url)
            url = self.base[:-10] + "sitemapindex"
            klass = XmlSitemapIndex
        else:
            logger.debug('unknown sitemaptype, switching to XML sitemap Index: '
                         + url)
            klass = XmlSitemapIndex
        try:
            logger.debug("SITEMAP: loading {}".format(url))
            return klass(url=url, html=self.html, base=self.base)
        except (AttributeError, TypeError, ValueError, urllib.error.URLError,
                urllib.error.HTTPError):
            logger.debug("SITEMAP: LOADING FAILED {}".format(url))
            return ()


class SitemapMixin(object):
    """
    Adds addition and iteration functionality.
    """
    xml = False
    head = False
    save_to_disk = True
    unique_tag = ""

    def __init__(self, url, html=None, base=None, filename=None, download=True):
        logger.debug('INIT SITEMAPMIXIN ' + url)
        self.links = []
        self.base = base
        self.modified_time = False
        self.publication_date = False
        self.revisit = False
        self.description = False
        try:
            super().__init__(url, html=html, base=base, save_file=True,
                             filename=None, download=download)
            self.unique_tag = self.namespace + self.unique_tag
        except urllib.error.HTTPError:
            logger.debug("SITEMAP @ {} DOESN'T EXIST; SKIPPED.".format(
                url))


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
        self.links += other.links
        self_attr, max_self, change_self = self._attr_len(self)
        other_attr, max_other, change_other = self._attr_len(other)
        self._update(self, max_other, self_attr, other_attr, change_self)
        self._update(other, max_self, other_attr, self_attr, change_other)
        return self

    def __iadd__(self, other):
        return self.__add__(other)

    @property
    def fits_xml(self):
        with open(self.filename, 'rb') as fileobj:
             context = etree.iterparse(fileobj, events=('end',),
                                       tag=self.unique_tag)
             try:
                 next(context)
                 return True
             except StopIteration:
                 return False


# class Rss(SitemapMixin, webpage.Webpage):
#     unique_tag = 'item'
#     tag = ["link", "lastBuildDate", "pubDate", "title", "description"]
#     name = ["links", "modified_time", 'publication_date', 'title',
#             'description']
#     parser = etree.XML
#     xml = True
#
#     def __init__(self, url, html=None, base=None):
#         super().__init__(url, html=html, base=base)
#         self.modified_time = [
#             mod_time if mod_time != '' else self.publication_date[i]
#             for i, mod_time in enumerate(self.modified_time)
#         ]


class XmlSitemap(SitemapMixin, webpage.Webpage):
    """
    Parses XML sitemapindexes.
    """
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]
    parser = etree.HTML
    xml = True
    next = None

    def __init__(self, url, html=None, base=None, filename=None, download=True):
        logger.debug('SITEMAP: loading XML ' + base)
        super().__init__(url, html=html, base=base, filename=filename,
                         download=download)
        if self.fits_xml:
            self._iterate_sitemaps = iter(self._fitting_sitemap_iterator())
        else:
            self._iterate_sitemaps = iter(self._next_sitemap_iterator(
                download=True))

    def __next__(self):
        x = next(self._iterate_sitemaps)
        return x

    def _next_sitemap_iterator(self, download):
        return self.next(
                url=self.url,
                base=self.base,
                download=download,
                filename=self.filename
            )


    def _fitting_sitemap_iterator(self):
        sitemap_dict = {}
        fi = 0
        while True:
            try:
                tag, d = next(self._iterator)
            except StopIteration:
                iterator = iter(self._sitemap(sitemap_dict, fi))
                for link in iterator:
                    yield link
                break
            name, value = list(d.items())[0]
            if name in sitemap_dict.keys():
                iterator = iter(self._sitemap(sitemap_dict, fi))
                for link in iterator:
                    yield link
                fi += 1
                sitemap_dict = {}
            sitemap_dict.update(d)

    def update_filename(self, fi):
        filebase = self.filename if not self.filename.endswith('.data') else \
            self.filename[:-5]
        return "{}_{}.data".format(filebase, str(fi))


class XmlUrlset(XmlSitemap):
    """
    Parses XML sitemaps.
    """
    unique_tag = 'url'
    # next = Rss

    def _sitemap(self, sitemap_dict, fi):
        yield sitemap_dict


class XmlSitemapIndex(XmlSitemap):
    """
    Parses XML sitemapindexes.
    """
    next = XmlUrlset
    unique_tag = 'sitemap'
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]

    def _sitemap(self, sitemap_dict, fi):
        link = sitemap_dict['links']
        links = [
            self.base + r'/sitemaps/' + link.split(r'/')[-1],
            self.base + r'/sitemap/' + link.split(r'/')[-1],
            link
        ]
        sitemap = self._try_sitemap(
            links=links,
            klass=self.next,
            filename=self.update_filename(fi)
        )
        for link in sitemap:
            yield link

    def _try_sitemap(self, links, klass, filename):
        try:
            link = links.pop()
            try:
                return klass(
                    url=link,
                    base=self.base,
                    filename=filename
                )
            except (TypeError, etree.XMLSyntaxError):
                logger.debug("Sitemap with link {} not working".format(link))
                return self._try_sitemap(links, klass, filename)
        except IndexError:
            logger.debug(
                "SITEMAP WITHOUT RESULTS WITH BASE: {}".format(
                    self.base)
            )
            return ()


class GunZip(XmlSitemap):
    """
    Parses gunzipped (.gz) XML sitmaps.
    """
    next = XmlSitemapIndex

    @property
    def fits_xml(self):
        return True

    def _fitting_sitemap_iterator(self):
        zipped = GzipFile(filename=self.filename, mode='rb')
        new_filename = self.update_filename(self.filename)
        with open(new_filename) as new_file:
            new_file.write(zipped.read())
        os.remove(self.filename)
        self.filename = new_filename
        for link in self._next_sitemap_iterator(download=False):
            yield link


class Html(SitemapMixin, webpage.Links):
    """
    Parses HTML sitemaps.
    """
    pass


class Txt(webpage.WebpageRaw):
    """
    Parses Txt sitemaps.
    """
    head = False
    visited = []

    def parse(self, *args, **kwargs):
        self.links = [y for x in self.html.split('\n') for y in x.split('\r')
                     if validate.url_explicit(y)]

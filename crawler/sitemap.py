# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import datetime
from gzip import GzipFile
import os
import urllib.error

import lxml.etree as etree

try:
    from base import logger_setup
    import model
    import webpage
    import validate
    from settings import CRAWL_DELAY
except ImportError:
    from crawler.base import logger_setup
    import crawler.model as model
    import crawler.webpage as webpage
    import crawler.validate as validate
    from crawler.settings import CRAWL_DELAY

logger = logger_setup(__name__)


class Sitemap(object):

    def __init__(self, urls, base, html=None, first=True, database_lock=None):
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
        self.database_lock = database_lock
        logger.debug('SITEMAP INITIALIZED: ' + self.base)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterator)

    def _iterator(self):
        while len(self.urls):
            url = self.urls.pop()
            sitemap = self.choose(url)
            for content in sitemap:
                yield content
            del sitemap
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
            return klass(url=url, html=self.html, base=self.base,
                         database_lock=self.database_lock)
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
    as_html = False

    def __init__(self, url, html=None, base=None, filename=None,
                 download=True, database_lock=None):
        logger.debug('INIT SITEMAPMIXIN {} as {}.'.format(
            url, self.__class__.__name__))
        self.links = []
        self.base = base
        self.modified_time = False
        self.publication_date = False
        self.revisit = False
        self.description = False
        super().__init__(
            url=url,
            html=html,
            base=base,
            save_file=True,
            filename=filename,
            download=download
        )
        self.unique_tag = self.namespace + self.unique_tag

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

    @property
    def fits_xml(self):
        with open(self.filename, 'rb') as fileobj:
            context = etree.iterparse(fileobj, events=('end',),
                                   tag=self.unique_tag)
            try:
                next(context)
                return True
            except (StopIteration, etree.XMLSyntaxError):
                return False


class XmlSitemap(SitemapMixin, webpage.Webpage):
    """
    Parses XML sitemapindexes.
    """
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]
    parser = etree.HTML
    xml = True
    next = None

    def __init__(self, url, html=None, base=None, filename=None,
                 download=True, database_lock=None):
        logger.debug('SITEMAP: loading XML ' + base)
        super().__init__(
            url=url,
            html=html,
            base=base,
            filename=filename,
            download=download,
            database_lock=database_lock
        )
        self.filenameindex = 0
        if not self.sitemap_crawlable:
            self._iterate_sitemaps = iter(())
        elif self.fits_xml:
            self._iterate_sitemaps = iter(self._fitting_sitemap_iterator())
        else:
            self._iterate_sitemaps = iter(self._next_sitemap_iterator(
                download=False, filename=self.filename))

    def __next__(self):
        try:
            x = next(self._iterate_sitemaps)
            return x
        except StopIteration:
            if self.sitemap_crawlable:
                self.store_sitemap()
            raise

    @property
    def sitemap_crawlable(self):
        """
        Website entry in database that belongs to this webpage.
        """
        with self.database_lock:
            recorded_sitemaps = self.session.query(
                model.SitemapHistory).filter_by(
                url=self.url).all()
            if not recorded_sitemaps:
                return True
            return min((x.modified - datetime.datetime.now()).days for x in
                recorded_sitemaps) > CRAWL_DELAY

    def _next_sitemap_iterator(self, download, filename=None):
        fn = filename if filename else self.update_filename()
        return self.next(
                    url=self.url,
                    base=self.base,
                    download=download,
                    filename=fn
                )

    def _fitting_sitemap_iterator(self):
        sitemap_dict = {}
        while True:
            try:
                d = next(self._iterator)
            except StopIteration:
                iterator = iter(self._sitemap(sitemap_dict))
                try:
                    for link in iterator:
                        yield link
                    del iterator
                except urllib.error.HTTPError:
                    logger.debug("SITEMAP @ {} DOESN'T EXIST; SKIPPED.".format(
                        self.url))
                break
            name, value = list(d.items())[0]
            if name in sitemap_dict.keys():
                iterator = iter(self._sitemap(sitemap_dict))
                try:
                    for link in iterator:
                        yield link
                    del iterator
                except urllib.error.HTTPError:
                    logger.debug("SITEMAP @ {} DOESN'T EXIST; SKIPPED.".format(
                        self.url))
                sitemap_dict = {}
            sitemap_dict.update(d)

    def update_filename(self):
        filebase = self.filename if not self.filename.endswith('.data') else \
            self.filename[:-5]
        fi = str(self.filenameindex)
        self.filenameindex += 1
        return "{}_{}.data".format(validate.filename(filebase, False), fi)

    def store_sitemap(self):
        with self.database_lock:
            website = self.website_entry
            new_item = model.SitemapHistory(
                url=self.url,
                modified=datetime.datetime.now()
            )
            website.sitemaps.append(new_item)
            self.store_model(item=website)
            logger.debug('Stored sitemap url: ' + self.url)


class XmlUrlset(XmlSitemap):
    """
    Parses XML sitemaps.
    """
    unique_tag = 'url'
    # next = Rss

    def _sitemap(self, sitemap_dict):
        yield sitemap_dict


class XmlSitemapIndex(XmlSitemap):
    """
    Parses XML sitemapindexes.
    """
    next = XmlUrlset
    unique_tag = 'sitemap'
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]

    def _sitemap(self, sitemap_dict):
        link = sitemap_dict['links']
        links = [
            self.base + r'/sitemaps/' + link.split(r'/')[-1],
            self.base + r'/sitemap/' + link.split(r'/')[-1],
            link
        ]
        sitemap = self._try_sitemap(
            links=links,
            klass=self.next,
            filename=self.update_filename()
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

    @property
    def namespace(self):
        return ''

    def _fitting_sitemap_iterator(self):
        zipped = GzipFile(filename=self.filename, mode='rb')
        new_filename = self.update_filename()
        with open(new_filename, 'w') as new_file:
            new_file.write(zipped.read().decode('utf-8'))
        os.remove(self.filename)
        for link in iter(self._next_sitemap_iterator(download=False,
                                                     filename=new_filename)):
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

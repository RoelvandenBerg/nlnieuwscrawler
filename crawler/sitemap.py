# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import copy
from gzip import GzipFile
import urllib.request as request
from zipfile import ZipFile

import lxml.etree as etree

import webpage


class SitemapMixin(object):
    """
    Adds a list of visited urls, addition and iteration functionality.
    """
    xml = False

    def __init__(self, url, html=None, base_url=None):
        super().__init__(url, html=html, base_url=base_url)
        self.visited = []

    def __add__(self, other):
        self.visited += other.visited
        self.links += other.links
        return self

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
                higher_sitemap = Xml(link)
                self += higher_sitemap
                self.visited.append(link)
            elif link not in self.visited:
                yield link
                self.visited.append(link)


class Html(SitemapMixin, webpage.Links):
    """
    Parses HTML sitemaps.
    """
    pass


class Xml(SitemapMixin, webpage.Webpage):
    """
    Parses XML sitemaps.
    """
    tag = ["loc", "lastmod", "changefreq"]
    name = ["links", "modified_time", "revisit"]
    parser = etree.XML
    xml = True


class Zip(Xml):
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
        self.old = Zip("")
        for key in unzipped_xmls.keys():
            self.html = unzipped_xmls[key]
            self_old = copy.deepcopy(self)
            self.parse(*args, **kwargs)
            self += self_old


class GunZip(Zip):
    """
    Parses gunzipped (.gz) XML sitmaps.
    """
    zip_method = GzipFile

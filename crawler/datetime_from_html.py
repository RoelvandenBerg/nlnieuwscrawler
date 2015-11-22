__author__ = 'roelvdberg@gmail.com'

import re

import dateutil.parser
from lxml import etree

from crawler.settings import DATE_TIME_DISTANCE

MONTHS = {
    'januari': 1,
    'jan': 1,
    'februari': 2,
    'feb': 2,
    'maart': 3,
    'mrt': 3,
    'april': 4,
    'apr': 4,
    'mei': 5,
    'juni': 6,
    'jun': 6,
    'juli': 7,
    'jul': 7,
    'augustus': 8,
    'aug': 8,
    'september': 9,
    'sept': 9,
    'oktober': 10,
    'okt': 10,
    'november': 11,
    'nov': 11,
    'december': 12,
    'dec': 12
}


class WebPageDateTime(object):
    datetime_regex = re.compile(
        r"\d{1,4}-\d{1,2}-\d{1,4}[T\s,.;:|<>-_]{0,%d}\d{1,2}:\d{1,2}"
        % DATE_TIME_DISTANCE
    )

    def __init__(self):
        self.method = self.parse
        self.default_args = []
        self.default_kwargs = {}

    def time_tag(self):
        pass

    def parse(self, html, htmltree):
        self.html = html
        self.htmltree = htmltree
        try:
            self.time = self.htmltree.xpath(r'.//time')[0]
        except IndexError:
            self.parse_time_regex()

    def parse_time_regex(self):
        datetime = self.datetime_regex.findall(self.html)
        datetimes = []
        for dt_ in datetime:
            try:
                dt_try = dateutil.parser.parse(dt_, fuzzy=True, dayfirst=True)
                datetimes.append(dt_try)
            except ValueError:
                pass
        self.find_xpath_for_datetime(datetime)
        return datetimes

    def in_tag(self, x):
        return re.compile(
            r'''<\s*\w+[\w\s="':;]*''' + x +
            r'''[\w\s="':;]*>.*</\s*\w+\s*>'''
        )

    def in_between_tag(self, x):
        return re.compile(
            r'''<\s*\w+[\w\s="':;]*>.*''' + x +
            r'''.*</\s*\w+\s*>'''
        )

    def shorten_xpath(self, xpath):
        splitted_xpath = xpath.split('/')
        current = ''
        while True:
            current = '/'.join([current, splitted_xpath.pop()]).strip('/')
            length = len(self.htmltree.findall(current))
            if length == 1:
                return current
            if length == 0:
                raise IndexError

    def find_xpath_for_datetime(self, datetime):
        # methods =
        print(datetime)
        for dt_ in datetime:
            element = self.in_tag(dt_).findall(self.html)
            attribute = None

            if not element:
                element = self.in_between_tag(dt_).findall(self.html)
                attribute = True
            if not element:
                element = self.datetime_regex.findall(self.html)
                attribute = False
            print(dt_, element, attribute)
            el = etree.HTML(element[0]).getchildren()[
                0].getchildren()[0]
            xpath = etree.ElementTree(self.htmltree).getpath(el)
            shortest_xpath = self.shorten_xpath(xpath)
            if attribute:
                pass
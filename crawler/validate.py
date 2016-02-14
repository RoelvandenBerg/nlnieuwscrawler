# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import re
import urllib.parse

try:
    from settings import NOFOLLOW
except ImportError:
    from crawler.settings import NOFOLLOW


# Regex taken from Django:
url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def url(url_):
    """
    Validate urls based on simple rules.
    - check for ms office types like .docx format
    - if the url has a three-letter-extension this should be htm, com, org, edu
      or gov

    :param url_: url to check
    :return: True if url is valid
    """
    url_extensions = ["htm", "com", "org", "edu", "gov"]
    url_ = url_.strip(r'/')
    try:
        docxtest = not (url_[-1] == 'x' and url_[-5] == ".")
    except IndexError:
        docxtest = True
    try:
        match = (url_[-3] in url_extensions or not url_[-4] == ".") \
                and docxtest \
                and not any(nofollowtxt in url_ for nofollowtxt in NOFOLLOW)
    except IndexError:
        return True
    return match


def url_explicit(url_):
    """
    Validate url based on regex and simple rules from url_validate.

    See url_validate and url_regex for the chosen rules.

    :param url_: url to check
    :return: True if url is valid
    """
    match = bool(url_regex.search(url_)) and url(url_)
    return match


import urllib.parse


def url_encode_non_ascii(b):
    x = re.sub('''[\x80-\xFF‘"';:]''', lambda c: '%%%02x' % ord(c.group(0)),
                  b.decode('utf-8'))
    return x


def iri_to_uri(iri):
    parts = urllib.parse.urlparse(iri)
    return urllib.parse.urlunparse([
        url_encode_non_ascii(part.encode('utf-8')) for part in parts
    ])

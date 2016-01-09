__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
import logging
import queue
import re
import threading
import urllib.error
import urllib.parse

import pybloof

try:
    import model
    from settings import *
    import validate
except ImportError:
    import crawler.model as model
    from crawler.settings import *
    import crawler.validate as validate


def logger_setup(name):
    # setup logger
    logger = logging.getLogger(name)
    print_logger = logging.StreamHandler()
    print_logger.setLevel(logging.DEBUG)
    logger.addHandler(print_logger)
    return logger


logger = logger_setup(__name__)


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
    [1]: link_queue: a queue of all links that still need to be crawled.
    """
    base_regex = re.compile(r"^(?:http)s?://[\w\-_\.]+", re.IGNORECASE)

    def __init__(self, base, database_lock, bloomfilter_size=33547705,
                 bloomfilter_hashes=23, bloomfilter_max=1000000):
        """
        :param base: either a string with a base url or a list of strings with
            base urls.
        """
        self.history = pybloof.StringBloomFilter(
            size=bloomfilter_size,
            hashes=bloomfilter_hashes
        )
        self.total_stored = 0
        self.max = bloomfilter_max
        self.database_lock = database_lock
        self.sitemap_semaphore = threading.Semaphore(MAX_CONCURRENT_SITEMAPS)
        super().__init__()
        self.session = model.Session()
        self += [[] for _ in range(CRAWL_DEPTH + 1)]
        self.lock = threading.RLock()
        self.base_queue = queue.Queue()
        if isinstance(base, str):
            base = [base]
        self.load_from_db = True
        sites = self.load_from_database()
        self.load_from_db = False
        for base_url in base:
            if base_url not in sites and base_url + '/' not in sites:
                self.append(base_url, 0)

    def load_from_database(self):
        sites = self.session.query(model.Website).all()
        sitelist = []
        for site in sites:
            sitelist.append(site.url)
            self.append(site.url, site.crawl_depth)
        pages = self.session.query(model.Webpage).all()
        now = dt.now()
        for page in pages:
            if (now - page.crawl_modified).days < REVISIT_AFTER:
                site = self.session.query(model.Website).filter_by(
                    id=page.website_id).one()
                self.add(page.url, site.crawl_depth, True)
                logger.debug('Not crawling: {}'.format(page.url))
        return sitelist

    def store(self, url, depth):
        """
        Stores url to website table in database.
        :param url: website base url
        """
        with self.database_lock:
            new_item = model.Website(
                url=url,
                created=dt.now(),
                modified=dt.now(),
                crawl_depth=depth,
            )
            self.session.add(new_item)
            self.session.commit()

    def url_belongs_to_base(self, url, base):
        """
        Makes sure mobile versions are also attributed to base url.
        :param url: url of webpage to check
        :param base: base url for website
        :return: boolean if url falls within base.
        """
        if url.startswith(r'http://m.') or base.startswith(r'http://m.'):
            url = url.strip(r'http://').strip('m.').strip('www.')
            base = base.strip(r'http://').strip('m.').strip('www.')
        return url.startswith(base)


    def add(self, url, current_depth, crawl_url=True):
        """
        Adds a url to self.

        :param url: regular url to be added.
        :param current_depth: depth at which the url has been harvested.
        """
        with self.lock:
            url = url.strip('/')
            # iterate over base urls at each depth in self to check if url is
            # in one of the base urls
            for i, link_bundle in enumerate(self):
                for base, link_queue in link_bundle:
                    # test if url contains base and hasn't been added before
                    if self.url_belongs_to_base(url, base):
                        if url not in self.history:
                            # link hasn't been added before, so store it
                            self.add_to_history(url)
                            if crawl_url:
                                link_queue.put(url)
                        return
            # link has not been matched to any of the base urls, so append it
            # as a base url.
            current_depth += 1
            self.append(url, current_depth)

    def add_to_history(self, url):
        self.history.add(url)
        self.total_stored += 1
        if self.total_stored > self.max:
            raise MemoryError('Too many urls stored in '
                              'bloomfilter')

    def append(self, url, depth):
        """
        Extract an urls base, and append the url and the base to self.

        :param url: a url to be added with its base.
        :param depth: crawl depth the url has to be added to.
        """
        if depth > CRAWL_DEPTH:
            return
        with self.lock:
            base = self.base_regex.findall(url)[0]
            if base not in self[depth] or validate.url_explicit(url):
                logger.debug('BASE_URL: adding new base @depth {} : {}'
                             .format(depth, base))
                link_queue = queue.Queue()
                link_queue.put(url)
                self.add_to_history(url)
                # base urls are added to the crawl queue only if set in
                # settings:
                if ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE:
                    link_queue.put(base)
                    self.add_to_history(base)
                self[depth].append((url, link_queue))
                self.base_queue.put((base, depth, link_queue))
                if not self.load_from_db:
                    self.store(base, depth)
            else:
                logger.debug("BASE_URL: cannot add {}".format(base))

    def add_links(self, link_container, depth=0, base_url=None):
        """
        Add a list of urls to self at a certain depth.

        :param link_container: list of urls
        :param depth: depth at which the urls have been harvested
        :param base_url: base at which the urls have been harvested
        """
        number_o_links = 0
        if not base_url:
            base_url = self.base[0]
        try:
            for url_ in link_container:
                if "#" in url_:
                    url_ = url_.split('#')[0]
                    if not len(url_):
                        continue
                if not url_.startswith("http"):
                    url_ = urllib.parse.urljoin(base_url, url_)
                if not validate.url_explicit(url_):
                    continue
                self.add(url_, depth)
                number_o_links += 1
        except AttributeError:
            logger.debug(
                'AttributeError while iterating over links @base {}'.format(
                    number_o_links, base_url)
            )
        logger.debug('{} links added @base {} .'.format(
            number_o_links, base_url))

    def __str__(self):
        return "BaseUrl with at depth 0: " + \
               ", ".join(self.base)

    def __repr__(self):
        return "\n".join(
            [
                "\nDEPTH {}:\n".format(str(i)) +
                "\n".join(
                    ["    - qsize: {} - {}\n".format(base[1].qsize(), base[0])
                     for base in layer]
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

# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
import logging
import queue
import re
import threading
import time
import urllib.error
import urllib.parse

import dateutil.parser

from settings import *
import validate
import model
import robot
import webpage


# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
print_logger = logging.StreamHandler()
print_logger.setLevel(logging.DEBUG)
logger.addHandler(print_logger)
logger.debug("NEW_CRAWL_RUN | " + dt.now().strftime('%H:%M | %d-%m-%Y |'))


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

    def __init__(self, base, database_lock):
        """
        :param base: either a string with a base url or a list of strings with
            base urls.
        """
        self.database_lock = database_lock
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

    def add(self, url, current_depth, add_to_history=False):
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
                            if not add_to_history:
                                link_queue.put(url)
                        return
            # link has not been matched to any of the base urls, so append it
            # as a base url.
            current_depth += 1
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
            if base not in self[depth] or validate.url_explicit(url):
                logger.debug('BASE_URL: adding new base @depth {} : {}'
                    .format(depth, base))
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
                if not validate.url_explicit(url_):
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

    def __init__(self, base, link_queue, historic_links, page=webpage.WebpageRaw,
                 base_url=None, depth=0, database_lock=None):
        """
        :param base: base url string .
        :param link_queue: queue from base url.
        :param historic_links: historic links from base url.
        :param page: WebPage class or one of its children.
        :param base_url: BaseUrl object that at least contains this website.
        :param depth: crawl depth of this website.
        """
        if not database_lock:
            self.database_lock = threading.RLock()
        else:
            self.database_lock = database_lock
        self.session = model.Session()
        self.base = base
        self.has_content = True
        self.robot_txt = robot.Txt(urllib.parse.urljoin(base, 'robots.txt'))
        self.robot_txt.read()
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = BaseUrl(base, self.database_lock)
            _, _, links = self.base_url.base_queue.get()
        self.links = link_queue
        self.depth = depth
        try:
            for i, link in enumerate(self.robot_txt.sitemap.links):
                if self.robot_txt.sitemap.xml:
                    try:
                        site = self.session.query(model.Webpage).filter_by(
                            url=link).order_by(
                            model.Webpage.crawl_modified).all()[-1]
                        modified = dateutil.parser.parse(
                            self.robot_txt.sitemap.modified_time[i])
                        if site.crawl_modified > modified:
                            with base_url.lock:
                                self.links.put(link)
                    except IndexError:
                        with base_url.lock:
                            self.links.put(link)
                else:
                    with base_url.lock:
                        self.links.put(link)
            with base_url.lock:
                historic_links += self.robot_txt.sitemap.links
        except AttributeError:
            pass
        self.webpage = page

    def _can_fetch(self, url_):
        """
        Tests if robots.txt accepts url_ as crawlable.
        :param url_: url to be tested
        :return: True or False
        """
        return self.robot_txt.can_fetch(USER_AGENT, url_)

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
                    self.robot_txt.crawl_delay + start_time - time.time())
            except:    # ONTZETTENDE HACK
                logger.debug('crawl.Website.run sleep hack is used!')

    def _run_once(self):
        """Runs one webpage of a website crawler."""
        logger.debug('WEBSITE: Running webpage: {url}'
                     .format(url=str(self.base)))
        link = self.links.get(timeout=1)
        if not self._can_fetch(link):
            logger.debug('WEBSITE: webpage {} cannot be fetched.'
                         .format(link))
            return
        try:
            page = self.webpage(
                url=link,
                base_url=self.base,
                database_lock=self.database_lock
            )
        except (urllib.error.HTTPError, UnicodeEncodeError):
            logger.debug('WEBSITE: HTTP error @ {}'.format(link))
            return
        if page.followable:
            urlfetcher = webpage.Links(url=link, base_url=self.base,
                                      html=page.html, download=False)
            self.base_url.add_links(urlfetcher, self.depth, self.base)
        else:
            logger.debug('WEBSITE: webpage not followable: {}'.format(link))
        if page.archivable:
            try:
                page.store()
            except (TypeError, AttributeError):
                logger.debug(
                    'WEBSITE: store content not working for page: {}'
                        .format(link))
        else:
            logger.warn('WEBSITE: webpage not archivable: {}'.format(link))
        if VERBOSE:
            logger.debug(page.content)


class Crawler(object):
    """
    Crawler that crawls sites based on a sitelist.
    Results are stored in a database defined in model.
    """
    def __init__(self, sitelist, page=webpage.WebpageRaw):
        """
        :param sitelist: a list of sites to be crawled.
        :param page: webpage class used for crawling
        """
        self.database_lock = threading.RLock()
        self.base_url = BaseUrl(sitelist, self.database_lock)
        self.websites = []
        self.webpage = page

    def run(self):
        """Run crawler"""
        number_of_website_threads = 1
        # Run while there is still active website-threads left.
        while number_of_website_threads > 0:
            # Run as much threads as MAX_THREADS (from settings) sets.
            while 0 < number_of_website_threads <= MAX_THREADS:
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
                number_of_website_threads = threading.activeCount() - 1
                logger.debug(
                    "CRAWLER: Number of threads with websites running: {}"
                    .format(number_of_website_threads)
                )
        logger.debug("CRAWLER: Finished")
        logger.debug("CRAWLER:\n" + repr(self.base_url))

    def _website_worker(self, base):
        """
        Worker that crawls one website.
        :param base: base instance from base_queue from a BaseUrl object.
        """
        site, depth, historic_links, link_queue = base
        logger.debug("CRAWLER: run for {} depth: {}".format(site, depth))
        website = Website(
            base=site,
            link_queue=link_queue,
            historic_links=historic_links,
            page=self.webpage,
            base_url=self.base_url,
            depth=depth,
            database_lock=self.database_lock
        )
        website.run()
        self.websites.append(website)


if __name__ == "__main__":
    standalone_crawler = Crawler(SITES, webpage.HeadingText)
    standalone_crawler.run()

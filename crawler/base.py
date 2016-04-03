import datetime
import logging
import os
import re
import threading
import urllib.error
import urllib.parse

# import pybloof
import pybloom.pybloom

__author__ = 'roelvdberg@gmail.com'

try:
    from filequeue import FileQueue
    from filequeue import Empty
    import model
    from settings import *
    import validate
except ImportError:
    from crawler.filequeue import FileQueue
    from crawler.filequeue import Empty
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


base_regex = re.compile(r"^(?:http)s?://[\w\-_\.]+", re.IGNORECASE)

def parse_base(url):
    return base_regex.findall(url)[0]


class BaseUrl(list):
    """
    A list of baseurls that can be crawled.

    The baseurl is the part of the url before the first / (apart from the two
    slashes after http(s):). BaseUrl is used to administrate which urls can be
    crawled, which still need to be crawled. Base urls are ordered
    according to their crawl depth. Where the first base urls have a depth of 0
    and urls found within a webpage that do not match that webpages' base url
    have an increased depth (by 1). Thus a BaseUrl has the following form:

    [{BASE_URL: , ...}n=0, {..., ...}n=1, {..., ...}n=2, ..., {}n=CRAWL_DEPTH]

    The CRAWL_DEPTH can be set in the settings file.

    :param lock: The BaseUrl has a lock that is used during threading. This way
        multiple threads can handle the BaseUrl.
    :param base_queue: a queue with websites that have not yet been crawled.

    Within a BaseUrl Each base url is stored as a list of parameters:
    [0]: the base url string
    [1]: link_queue: a queue of all links that still need to be crawled.
    """

    def __init__(self, base, database_lock, bloomfilter_size=33547705,
                 bloomfilter_hashes=23, bloomfilter_max=1000000):
        """
        :param base: either a string with a base url or a list of strings with
            base urls.
        """
        self.history = pybloom.pybloom.ScalableBloomFilter(
            initial_capacity=bloomfilter_max,
            error_rate=0.0001,
            mode=pybloom.pybloom.ScalableBloomFilter.SMALL_SET_GROWTH
        )
        # self.history = pybloof.StringBloomFilter(
        #     size=bloomfilter_size,
        #     hashes=bloomfilter_hashes
        # )
        self.total_stored = 0
        self.max = bloomfilter_max
        self.database_lock = database_lock
        self.sitemap_semaphore = threading.Semaphore(MAX_CONCURRENT_SITEMAPS)
        super().__init__()
        self.session = model.Session()
        self += [{} for _ in range(CRAWL_DEPTH + 1)]
        self.lock = threading.RLock()
        self.base_queue = FileQueue(
            directory="../data",
            name='base_url',
            persistent=True,
            overwrite=True
        )
        if isinstance(base, str):
            base = [base]
        self.load_from_db = True
        sites = self.load_from_database()
        self.load_from_db = False
        for base_url in base:
            if base_url not in sites and base_url + '/' not in sites and \
                    base_url not in self.history:
                self.append(base_url, 0)

    def load_from_database(self):
        sites = self.session.query(model.Website).all()
        sitelist = []
        for site in sites:
            sitelist.append(site.url)
            if not site.url in self.history:
                self.append(site.url, site.crawl_depth)
        pages = self.session.query(model.Webpage).all()
        now = datetime.datetime.now()
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
                created=datetime.datetime.now(),
                modified=datetime.datetime.now(),
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
            for i, base_bundle in enumerate(self):
                for base, link_queue in base_bundle.items():
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
        if self.total_stored == self.max + 1:
            logger.debug('Too many urls stored in bloomfilter. now '
                         'stores more than {} urls.'.format(self.max))

    def append(self, url, depth):
        """
        Extract an urls base, and append the url and the base to self.

        :param url: a url to be added with its base.
        :param depth: crawl depth the url has to be added to.
        """
        if depth > CRAWL_DEPTH:
            return
        with self.lock:
            base = parse_base(url)
            if base not in self[depth] or validate.url_explicit(url):
                logger.debug('BASE_URL: adding new base @depth {} : {}'
                             .format(depth, base))
                if '//' in base:
                    queue_name = base.split('//')[1]
                else:
                    queue_name = base
                directory = '../data'
                dir_exists = os.path.exists(os.path.join(directory, queue_name))
                if dir_exists:
                    temp_link_queue = FileQueue(
                        directory="../data",
                        name='temp' + queue_name,
                        persistent=False,
                        overwrite=True,
                        pickled=False
                    )
                    link_queue = FileQueue(
                        directory="../data",
                        name=queue_name,
                        persistent=False,
                        overwrite=True,
                        pickled=False
                    )
                    while True:
                        try:
                            existing_link = link_queue.get()
                        except (StopIteration, Empty):
                            break
                        self.history.put(existing_link)
                        temp_link_queue.put(existing_link)
                link_queue = FileQueue(
                    directory="../data",
                    name=queue_name,
                    persistent=True,
                    overwrite=True,
                    pickled=False
                )
                if dir_exists:
                    while True:
                        try:
                            link_queue.put(temp_link_queue.get())
                        except (StopIteration, Empty):
                            break
                self.add_to_history(url)
                link_queue.put(url)
                # base urls are added to the crawl queue only if set in
                # settings:
                if ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE:
                    self.add_to_history(base)
                    link_queue.put(base)
                self[depth][base] = link_queue
                self.base_queue.put((base, depth))
                if not self.load_from_db:
                    self.store(base, depth)
            else:
                logger.debug("BASE_URL: cannot add {}".format(base))

    def add_links(self, link_container, depth=0, base=None):
        """
        Add a list of urls to self at a certain depth.

        :param link_container: list of urls
        :param depth: depth at which the urls have been harvested
        :param base: base at which the urls have been harvested
        """
        number_of_links = 0
        if not base:
            base = self.base[0]
        # try:
        for url_dict in link_container:
            url = url_dict['links']
            if "#" in url:
                url = url.split('#')[0]
                if not len(url):
                    continue
            url = urllib.parse.urljoin(base, url)
            if not validate.url_explicit(url):
                continue
            self.add(url, depth)
            number_of_links += 1
        logger.debug('{} links added @base {} .'.format(
            number_of_links, base))

    def __str__(self):
        return "BaseUrl with at depth 0: " + \
               ", ".join(self.base)

    def __repr__(self):
        return "\n".join(
            [
                "\nDEPTH {}:\n".format(str(i)) +
                "\n".join(
                    ["    - qsize: {} for {}\n".format(link_queue.qsize(), base)
                     for base, link_queue in layer.items()]
                ) for i, layer in enumerate(self)
            ]
        )

    @property
    def base(self):
        """
        List of base urls at lowest depth.
        """
        return [x for x, _ in self[0].items()]

    @property
    def has_content(self):
        """
        Indicates whether any of the base urls still has links in their queue.
        """
        try:
            return any(any(q.qsize() > 0 for q in l.values()) for l in self)
        except IndexError:
            return False

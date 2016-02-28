import datetime
import logging
import re
import threading
import urllib.error
import urllib.parse

__author__ = 'roelvdberg@gmail.com'

try:
    from filequeue import FileQueue
    import model
    from settings import *
    import validate
except ImportError:
    from crawler.filequeue import FileQueue
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


class Empty(Exception):
    pass


class WebpageDatabaseQueue(object):

    def __init__(self, base, database_lock):
        self.session = model.Session()
        self.database_lock = database_lock
        self.base = base

    def get(self):
        with self.database_lock:
            website_entry = self.website_entry
            uncrawled = self.session.query(model.CrawledLinks).filter_by(
                website_id=website_entry.id,
                crawled_at=None
            )
            try:
                next_url = uncrawled[0]
            except IndexError:
                uncrawled = self.session.query(model.CrawledLinks).filter_by(
                    website_id=website_entry.id,
                ).filter(
                    model.CrawledLinks.crawled_at < (
                        datetime.datetime.now() - datetime.timedelta(
                            days=CRAWL_DELAY))
                ).all()
                uncrawled.sort(key=lambda x: x.crawled_at)
                try:
                    next_url = uncrawled[0]
                except IndexError:
                    raise Empty
            next_url.crawled_at = datetime.datetime.now()
            next_url.modified = datetime.datetime.now()
            self.session.add(next_url)
            self.session.commit()
            return next_url.url

    def put(self, url):
        with self.database_lock:
            website = self.website_entry
            new_entry = model.CrawledLinks(
                url=url,
                modified=datetime.datetime.now()
            )
            website.crawled_links.append(new_entry)
            self.session.add(website)
            self.session.commit()

    def __contains__(self, item):
        with self.database_lock:
            query_result = self.session.query(model.CrawledLinks).filter_by(
                url=item
            ).all()
        return bool(query_result)

    @property
    def website_entry(self):
        """
        Website entry in database that belongs to this Queue.
        """
        with self.database_lock:
            return self.session.query(model.Website).filter_by(
                url=self.base).one()


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
    __contains__ = WebpageDatabaseQueue.__contains__

    def __init__(self, base, database_lock):
        """
        :param base: either a string with a base url or a list of strings with
            base urls.
        """
        self.total_stored = 0
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
        for base_url in base:
            self.append(base_url, 0)

    def store(self, url, depth):
        """
        Stores url to website table in database.
        :param url: website base url
        """
        if not self.website_entry_exists(url):
            with self.database_lock:
                new_item = model.Website(
                    url=url,
                    created=datetime.datetime.now(),
                    modified=datetime.datetime.now(),
                    crawl_depth=depth,
                )
                self.session.add(new_item)
                self.session.commit()

    def website_entry_exists(self, base):
        with self.database_lock:
            query_result = self.session.query(model.Website).filter_by(
                url=base
            ).all()
            return bool(query_result)

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
                        if url not in self:
                            # link hasn't been added before, so store it
                            if crawl_url:
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
            base = parse_base(url)
            if base not in self[depth] or validate.url_explicit(url):
                logger.debug('BASE_URL: adding new base @depth {} : {}'
                             .format(depth, base))
                self.store(base, depth)
                link_queue = WebpageDatabaseQueue(
                    base=base,
                    database_lock=self.database_lock
                )
                link_queue.put(url)
                # base urls are added to the crawl queue only if set in
                # settings:
                if ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE:
                    link_queue.put(base)
                self[depth][base] = link_queue
                self.base_queue.put((base, depth))
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

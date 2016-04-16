import datetime
import gc
import gzip
import logging
import os.path
import shutil
import pybloom.pybloom
import re
import threading
import time
import urllib.parse
import urllib.request

import lxml
import lxml.etree

try:
    import filequeue
    from settings import USER_AGENT, SITES, MAX_CONCURRENT_SITEMAPS, \
        MAX_THREADS, CRAWL_DELAY, LOG_FILENAME, ROBOT_NOFOLLOW
    import validate
    import robot
except ImportError:
    import crawler.filequeue as filequeue
    from crawler.settings import USER_AGENT, SITES, \
        MAX_CONCURRENT_SITEMAPS, MAX_THREADS, CRAWL_DELAY, LOG_FILENAME, \
        ROBOT_NOFOLLOW
    import crawler.validate as validate
    import crawler.robot as robot


def logger_setup(name):
    # setup logger
    logger = logging.getLogger(name)
    print_logger = logging.StreamHandler()
    print_logger.setLevel(logging.DEBUG)
    logger.addHandler(print_logger)
    return logger

logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logger = logger_setup(__name__)

base_regex_http = re.compile(r"^(https?://[\w\-_\.]+)", re.IGNORECASE)
base_regex = re.compile(r"^(?:https?://)([\w\-_\.]+)", re.IGNORECASE)
file_regex = re.compile(r"^(https?://[\w\-_\.]+/?)", re.IGNORECASE)


def parse_base(url, http=False):
    if http:
        return base_regex_http.findall(url)[0]
    return base_regex.findall(url)[0]


def parse_filename(url):
    filename = file_regex.sub("", url)
    if not filename:
        filename = parse_base(url)
    return validate.filename(filename)


def base_filename(url):
    return validate.filename(parse_base(url))


def download_to_disk(url, data_dir='data'):
    """
    Fetches the content of a webpage, based on an url.

    Apart from the arguments described below, extra arguments and key-value
    pairs will be handed over to the parse method. View that method for
    the extra possible parameters.

    :param url: the url which content will be downloaded.
    """
    header = {'User-Agent': USER_AGENT}
    url = validate.iri_to_uri(url)
    filename = parse_filename(url).strip('/') + ".crawled"
    base_directory = base_filename(url)
    path = os.path.join(data_dir, base_directory, filename)
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # download to disk
    with urllib.request.urlopen(urllib.request.Request(url, headers=header)) as\
            response, open(path, 'wb') as f:
        f.write(response.read())
    logger.debug(
        '%s Saving %s to disk. Parsing from disk',
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
        filename
    )
    return path, url


def file_iter(filename, tags, as_html=True):
    """
    fast_iter is useful if you need to free memory while iterating through a
    very large XML file.

    http://lxml.de/parsing.html#modifying-the-tree
    Based on Liza Daly's fast_iter
    http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    See also http://effbot.org/zone/element-iterparse.htm

    :returns: current tag, and a dictionary with values for each given
        name. {name: value, ...}
    """
    if not hasattr(tags, '__iter__'):
        tags = [tags]
    with open(filename, 'rb') as fileobj:
        context = lxml.etree.iterparse(fileobj, events=('end',), tag=tags,
                                  html=as_html)
        for event, elem in context:
            yield elem
            # It's safe to call clear() here because no descendants will be
            # accessed
            elem.clear()
            # Also eliminate now-empty references from the root node to elem
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del context


def url_belongs_to_base(url, base):
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


def add_url(base, new_url, history, history_lock, url_queue, robot_txt):
    try:
        if url_belongs_to_base(new_url, base):
            new_base = base
        else:
            new_base = parse_base(new_url, http=True)
    except IndexError:
        new_base = base
        new_url = urllib.parse.urljoin(base, new_url)
    if "#" in new_url:
        new_url = new_url.split('#')[0]
        if not len(new_url):
            return
    if not validate.url_explicit(new_url):
        return
    if not new_url in history:
        with history_lock:
            history.add(new_url)
        if new_base in SITES and robot_txt.can_fetch(USER_AGENT, new_url):
            url_queue.put(new_url)


def namespace(filename):
    try:
        with open(filename, 'rb') as fileobj:
            context = lxml.etree.iterparse(fileobj, events=('end',))
            namespace_ = next(context)[1].nsmap
            # logger.debug('NAMESPACE: ' + str(namespace))
            return "{" + namespace_[None] + "}"
    except (KeyError, lxml.etree.XMLSyntaxError):
        return ""


def fits_xml(filename):
    with open(filename, 'rb') as fileobj:
        context = lxml.etree.iterparse(fileobj, events=('end',),
                                       tag=namespace(filename) + 'url')
        try:
            next(context)
            return parse_xml_urlset
        except (StopIteration, lxml.etree.XMLSyntaxError):
            return parse_xml_sitemapindex


def parse_xml_sitemapindex(path, sitemap_queue, **kwargs):
    tag = namespace(path) + 'loc'
    for elem in file_iter(path, tag, False):
        sitemap_url = elem.text
        sitemap_queue.put(sitemap_url)
    os.remove(path)


def parse_xml_urlset(base, path, url_queue, history, history_lock, robot_txt,
                     **kwargs):
    tag = namespace(path) + 'loc'
    for elem in file_iter(path, tag, False):
        new_url = elem.text
        add_url(base, new_url, history, history_lock, url_queue, robot_txt)
    os.remove(path)


def unzip_gzip(filename):
    zipped = gzip.GzipFile(filename=filename, mode='rb')
    new_filename = filename + 'unzipped'
    with open(new_filename, 'w') as new_file:
        new_file.write(zipped.read().decode('utf-8'))
    os.remove(filename)
    return new_filename


def download_sitemap(url, base, data_dir):
    # determine what type of sitemap it entails.
    if url == '/sitemapindex/':
        logger.debug('%s XML sitemap chosen: %s',
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), url)
        url = urllib.parse.urljoin(base, "sitemapindex")
        parser = parse_xml_sitemapindex
    path, _ = download_to_disk(url, data_dir=data_dir)
    if url.endswith('.gz'):
        logger.debug('%s GunZip sitemap chosen: %s',
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), url)
        path = unzip_gzip(path)
        parser = fits_xml(path)
    elif url.endswith('.xml') or (url.startswith('google') and
                                  url.endswith('map')):
        logger.debug('%s XML sitemap chosen: %s',
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), url)
        parser = fits_xml(path)
    elif url == '/sitemapindex/':
        pass
    else:
        logger.debug(
            '%s unknown sitemaptype, switching to XML sitemap Index: %s',
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), url)
        parser = fits_xml(path)
    return url, parser, path


def sitemap_worker(base, sitemap_queue, url_queue, history, history_lock,
                   robot_txt, data_dir='data/sitemaps'):
    while True:
        delay = time.time()
        try:
            sitemap_url = sitemap_queue.get()
            logger.debug(
                '%s loading sitemap: %s',
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                sitemap_url
            )
        except filequeue.Empty:
            # logger.debug(
            #     "%s Base %s has no more sitemaps",
            #     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), base)
            break
        url, sitemap_parser, path = download_sitemap(
            sitemap_url, base, data_dir)
        logger.debug(
            '%s parsing sitemap: %s at path: %s',
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
            url, path
        )
        sitemap_parser(
            path=path,
            base=base,
            url_queue=url_queue,
            sitemap_queue=sitemap_queue,
            history=history,
            history_lock=history_lock,
            robot_txt=robot_txt
        )
        try:
            time.sleep(delay + CRAWL_DELAY - time.time())
        except ValueError:
            pass


def iter_hyperlinks(path):
    for elem in file_iter(path, 'a', True):
        href = elem.get('href')
        robots = elem.get('robots')
        if href and robots not in ROBOT_NOFOLLOW:
            yield href
        elif href:
            logger.debug('%s crawling not allowed for: %s by robots attribute',
                         datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                         href)


def webpage_worker(base, url_queue, history, history_lock, robot_txt,
                   data_dir='data'):
    if base is None:
        raise TypeError('Base {} is None!'.format(base))
    while True:
        delay = time.time()
        try:
            url = url_queue.get()
            logger.debug(
                '%s Crawling %s',
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                url
            )
        except filequeue.Empty:
            logger.debug(
                '%s %s worker empty, stopping',
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), base)
            break  # This can be problematic when larger crawl depth is allowed
        try:
            path, url = download_to_disk(url, data_dir=data_dir)
        except urllib.error.HTTPError:
            logger.error(
                '%s url %s raises HTTP statuscode',
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), url)
            continue
        with history_lock:
            with open(os.path.join(data_dir, 'crawled.csv'), 'a') as crawled:
                crawled_line = [
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                    url, path
                ]
                crawled.write(';'.join(crawled_line) + "\n")
        logger.debug('%s %s saved to disk',
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), path)
        for new_url in iter_hyperlinks(path):
            add_url(base, new_url, history, history_lock, url_queue,
                    robot_txt)
        try:
            time.sleep(delay + CRAWL_DELAY - time.time())
        except ValueError:
            pass


class Crawler(object):

    def __init__(self, bloomfilter_max=100000):
        self.sitemap_base_queue = filequeue.FileQueue(
            directory="data/queues/", name='sitemap_base_queue')
        self.base_queue = filequeue.FileQueue(directory="data/queues/",
                                              name='base_queue')
        self.url_queues = {}
        self.sitemap_queues = {}
        self.sitemap_semaphore = threading.Semaphore(MAX_CONCURRENT_SITEMAPS)
        self.crawl_semaphore = threading.Semaphore(MAX_THREADS)
        self.history = pybloom.pybloom.ScalableBloomFilter(
                initial_capacity=bloomfilter_max,
                error_rate=0.0001,
                mode=pybloom.pybloom.ScalableBloomFilter.SMALL_SET_GROWTH
            )
        self.history_lock = threading.RLock()
        self.robots = {}
        for site in SITES:
            logger.debug(
                '%s initializing: %s',
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                site
            )
            site = parse_base(site, http=True)
            if site is None:
                raise TypeError('Base {} is None!'.format(site))
            filename = base_filename(site)
            self.base_queue.put(site)
            self.sitemap_base_queue.put(site)
            url_queue = filequeue.FileQueue(directory="data/queues/",
                                            name=filename + '.queue',
                                            persistent=True, overwrite=True)
            sitemap_queue = filequeue.FileQueue(directory="data/queues/",
                                                name='sitemap_' + filename)
            url_queue.put(site)
            self.url_queues[site] = url_queue
            with self.history_lock:
                self.history.add(site)
            site_robot = robot.Txt(urllib.parse.urljoin(site, 'robots.txt'))
            site_robot.read(sitemap_queue)
            self.sitemap_queues[site] = sitemap_queue
            self.robots[site] = site_robot

    def run(self, sitemaps=True, pages=True):
        if sitemaps:
            self.collect_sitemaps()
        if pages:
            self.crawl_pages()

    def collect_sitemaps(self):
        threads_running = 1
        all_sitemaps_started = False
        while threads_running:
            if not all_sitemaps_started:
                with self.sitemap_semaphore:
                    try:
                        base = self.sitemap_base_queue.get()
                    except filequeue.Empty:
                        logger.debug('%s all sitemap threads have started.',
                            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                        )
                        all_sitemaps_started = True
                        continue
                    logger.debug(
                        '%s starting sitemap %s',
                        datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                        base
                    )
                    thread = threading.Thread(
                        target=sitemap_worker,
                        args=(
                            base, self.sitemap_queues[base], self.url_queues[base],
                            self.history, self.history_lock, self.robots[base]
                        )
                    )
                    thread.start()
            threads_running = threading.activeCount() - 1
        del self.sitemap_semaphore
        del self.sitemap_queues
        del self.sitemap_base_queue
        gc.collect()
        logger.debug("%s Sitemaps collected",
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

    def crawl_pages(self):
        threads_running = 1
        all_pages_started = False
        while threads_running:
            if not all_pages_started:
                with self.crawl_semaphore:
                    try:
                        base = self.base_queue.get()
                        if base is None:
                            raise TypeError('Base {} is None!'.format(base))
                    except filequeue.Empty:
                        logger.debug('%s all sitemap threads have started.',
                            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                        )
                        all_pages_started = True
                        continue
                    thread = threading.Thread(
                        target=webpage_worker,
                        args=(base, self.url_queues[base], self.history,
                              self.history_lock, self.robots[base])
                    )
                    thread.start()
            threads_running = threading.activeCount() - 1
        logger.debug("%s Webpages collected",
                     datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))


if __name__ == "__main__":
    try:
        shutil.rmtree('data')
    except FileNotFoundError:
        pass
    crawler = Crawler()
    crawler.run(sitemaps=True, pages=True)


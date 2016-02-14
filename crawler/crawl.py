# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

from datetime import datetime as dt
import logging
import threading
import time
import urllib.error
import urllib.parse

try:
    import base as base_
    from filequeue import Empty
    import model
    import robot
    from settings import *
    import validate
    import webpage
    from webpage import remove_file
except ImportError:
    import crawler.base as base_
    from crawler.filequeue import Empty
    import crawler.model as model
    import crawler.robot as robot
    from crawler.settings import *
    import crawler.validate as validate
    import crawler.webpage as webpage
    from crawler.webpage import remove_file

logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logger = base_.logger_setup(__name__)
logger.debug("NEW_CRAWL_RUN | " + dt.now().strftime('%H:%M | %d-%m-%Y |'))


ENCODINGS = ['utf_8', 'latin_1', 'utf_16', 'utf_16_be', 'utf_16_le', 'utf_32',
             'utf_32_be', 'utf_32_le', 'utf_7', 'base64_codec', 'big5',
             'big5hkscs', 'bz2_codec', 'cp037', 'cp1026', 'cp1125', 'cp1140',
             'cp1250', 'cp1251', 'cp1252', 'cp1253', 'cp1254', 'cp1255',
             'cp1256', 'cp1257', 'cp1258', 'cp273', 'cp424', 'cp437',
             'cp500',  'cp775', 'cp850', 'cp852', 'cp855', 'cp857', 'cp858',
             'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865', 'cp866',
             'cp869', 'cp932', 'cp949', 'cp950', 'euc_jis_2004', 'euc_jisx0213',
             'euc_jp', 'euc_kr', 'gb18030', 'gb2312', 'gbk', 'hex_codec',
             'hp_roman8', 'hz', 'iso2022_jp', 'iso2022_jp_1', 'iso2022_jp_2',
             'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext', 'iso2022_kr',
             'iso8859_10', 'iso8859_11', 'iso8859_13', 'iso8859_14',
             'iso8859_15', 'iso8859_16', 'iso8859_2', 'iso8859_3', 'iso8859_4',
             'iso8859_5', 'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9',
             'johab', 'koi8_r', 'mac_cyrillic', 'mac_greek', 'mac_iceland',
             'mac_latin2', 'mac_roman', 'mac_turkish', 'mbcs', 'ptcp154',
             'quopri_codec', 'rot_13', 'shift_jis', 'shift_jis_2004',
             'shift_jisx0213', 'tactis', 'tis_620', 'uu_codec', 'zlib_codec',
             'ascii']


ENCODINGS = list(reversed(ENCODINGS))


class Website(object):
    """
    Website crawler that crawls all pages in a website.
    """

    def __init__(self, base, link_queue, page=webpage.WebpageRaw,
                 base_url=None, depth=0, database_lock=None):
        """
        :param base: base url string .
        :param link_queue: queue from base url.
        :param page: WebPage class or one of its children.
        :param base_url: BaseUrl object that at least contains this website.
        :param depth: crawl depth of this website.
        """
        self.encoding = ENCODINGS[:]
        if not database_lock:
            self.database_lock = threading.RLock()
        else:
            self.database_lock = database_lock
        self.session = model.Session()
        self.base = base
        self.has_content = True
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = base_.BaseUrl(base=base,
                                         database_lock=self.database_lock)
        self.robot_txt = robot.Txt(
            url=urllib.parse.urljoin(base, 'robots.txt'),
            base_url=self.base_url
        )
        try:
            self.robot_txt.read()
        except Exception as e:
            logger.exception("Error: {} @webpage with base {}".format(
                e, self.base))
        self.links = link_queue
        self.depth = depth
        self.base_url.add_links(
            link_container=self.robot_txt.sitemap,
            depth=self.depth,
            base=self.base
        )
        logger.debug('SITEMAP READ FOR: ' + self.base)
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
            except Empty:
                self.has_content = False
            except Exception as e:
                logger.exception("Error: {} @webpage with base {}".format(
                    e, self.base))
            try:
                wait_time_left = self.robot_txt.crawl_delay + start_time - \
                             time.time()
                time.sleep(wait_time_left)
            except (ValueError, IOError):
                wait_time_left = 1
                while wait_time_left > 0:
                    wait_time_left = self.robot_txt.crawl_delay + start_time - \
                             time.time()

    def _run_once(self):
        """Runs one webpage of a website crawler."""
        logger.debug('WEBSITE: Running webpage: {url}'
                     .format(url=str(self.base)))
        link = self.links.get()
        if not self._can_fetch(link):
            logger.debug('WEBSITE: webpage {} cannot be fetched.'
                         .format(link))
            return
        filename = '../data/thread_{}_{}.data'.format(
            self.base.split('.')[-2].split('/')[-1], link.split('/')[-1])
        while True:
            try:
                page = self.webpage(
                    url=link,
                    base=self.base,
                    database_lock=self.database_lock,
                    encoding=self.encoding[-1],
                    save_file=True,
                    filename=filename,
                    persistent=True
                )
                if page.followable:
                    urlfetcher = webpage.Links(
                        url=link,
                        base=self.base,
                        html=page.html,
                        download=False,
                        save_file=True,
                        filename=page.filename,
                        persistent=True
                    )
                    self.base_url.add_links(
                        link_container=urlfetcher,
                        depth=self.depth,
                        base=self.base
                    )
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
            except urllib.error.HTTPError:
                logger.debug('WEBSITE: HTTP error @ {}'.format(link))
                remove_file(filename)
                return
            except UnicodeDecodeError:
                self.encoding.pop()
                time.sleep(CRAWL_DELAY)
                continue
            break
        remove_file(filename)
        if page.encoding != self.encoding[-1]:
            self.encoding.append(page.encoding)
        del urlfetcher
        del page


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
        self.base_url = base_.BaseUrl(sitelist, self.database_lock)
        self.websites = []
        self.webpage = page

    def run(self):
        """Run crawler"""
        number_of_website_threads = 1
        # Run while there are still active website-threads left.
        while number_of_website_threads > 0:
            # Run as much threads as MAX_THREADS (from settings) sets.
            while 0 < number_of_website_threads <= MAX_THREADS:
                # start a new website thread:
                self.run_once()
                number_of_website_threads = threading.activeCount() - 1
        logger.debug("CRAWLER: Finished")
        logger.debug("CRAWLER:\n" + repr(self.base_url))

    def run_once(self):
        try:
            base_url_queue_item = self.base_url.base_queue.get()
            thread = threading.Thread(
                target=self._website_worker,
                args=(base_url_queue_item,)
            )
            thread.start()
        except Empty:
            pass #logger.debug("Queue of base urls is empty.")

    def _website_worker(self, base_url_queue_item):
        """
        Worker that crawls one website.
        :param base: base instance from base_queue from a BaseUrl object.
        """
        base, depth = base_url_queue_item
        link_queue = self.base_url[depth][base]
        logger.debug("CRAWLER: run for {} depth: {}".format(base, depth))
        website = Website(
            base=base,
            link_queue=link_queue,
            page=self.webpage,
            base_url=self.base_url,
            depth=depth,
            database_lock=self.database_lock
        )
        website.run()
        self.websites.append(website)


if __name__ == "__main__":
    dutch_news_crawler = Crawler(SITES)
    dutch_news_crawler.run()

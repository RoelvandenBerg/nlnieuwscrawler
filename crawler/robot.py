# -*- coding: utf-8 -*-
__author__ = 'roelvdberg@gmail.com'

import urllib.parse
import urllib.robotparser as robotparser


try:
    from crawl import logger_setup
except ImportError:
    from crawler.crawl import logger_setup


logger = logger_setup(__name__)


class Txt(robotparser.RobotFileParser):
    """
    Extention of robotparser, adds sitemap functionality, mainly a copy.

    Additions:
    - sitemaps
    - logging
    """

    def read(self, sitemap_queue):
        """Reads the robots.txt URL and feeds it to the parser."""
        try:
            f = urllib.request.urlopen(self.url)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400 and err.code < 500:
                self.allow_all = True
        else:
            raw = f.read()
            sitemap_queue = self.parse(
                raw.decode("utf-8").splitlines(), sitemap_queue)
        finally:
            return sitemap_queue

    def parse(self, lines, sitemap_queue):
        """Parse the input lines from a robots.txt file.

        We allow that a user-agent: line is not preceded by
        one or more blank lines.
        """
        # states:
        #   0: start state
        #   1: saw user-agent line
        #   2: saw an allow or disallow line
        state = 0
        entry = robotparser.Entry()

        self.modified()
        for line in lines:
            if not line:
                if state == 1:
                    entry = robotparser.Entry()
                    state = 0
                elif state == 2:
                    self._add_entry(entry)
                    entry = robotparser.Entry()
                    state = 0
            # remove optional comment and strip line
            i = line.find('#')
            if i >= 0:
                line = line[:i]
            line = line.strip()
            if not line:
                continue
            line = line.split(':', 1)
            if len(line) == 2:
                line[0] = line[0].strip().lower()
                line[1] = urllib.parse.unquote(line[1].strip())
                if line[0] == "user-agent":
                    if state == 2:
                        self._add_entry(entry)
                        entry = robotparser.Entry()
                    entry.useragents.append(line[1])
                    state = 1
                elif line[0] == "disallow":
                    if state != 0:
                        entry.rulelines.append(robotparser.RuleLine(line[1],
                                                                    False))
                        state = 2
                elif line[0] == "allow":
                    if state != 0:
                        entry.rulelines.append(robotparser.RuleLine(line[1],
                                                                    True))
                        state = 2
                elif line[0] == 'sitemap':
                    sitemap_url = line[1]
                    logger.debug('adding sitemap %s', sitemap_url)
                    sitemap_queue.put(sitemap_url)
                elif line[0].lower().startswith('crawl-delay'):
                    new_delay = float(line[1])
                    if self.crawl_delay < new_delay:
                        self.crawl_delay = new_delay
        if state == 2:
            self._add_entry(entry)
        return sitemap_queue

    def can_fetch(self, useragent, url):
        """using the parsed robots.txt decide if useragent can fetch url"""
        if self.disallow_all:
            logger.debug('dissalow all for {}'.format(url))
            return False
        if self.allow_all:
            return True
        # Until the robots.txt file has been read or found not
        # to exist, we must assume that no url is allowable.
        # This prevents false positives when a user erronenously
        # calls can_fetch() before calling read().
        if not self.last_checked:
            logger.debug('last_checked unset for {}'.format(url))
            return False
        # search for given user agent matches
        # the first match counts
        parsed_url = urllib.parse.urlparse(urllib.parse.unquote(url))
        url = urllib.parse.urlunparse(('', '', parsed_url.path,
                                       parsed_url.params, parsed_url.query,
                                       parsed_url.fragment))
        url = urllib.parse.quote(url)
        if not url:
            url = "/"
        for entry in self.entries:
            if entry.applies_to(useragent):
                fetchable = entry.allowance(url)
                if not fetchable:
                    logger.debug('user agent not allowed for {}'.format(url))
                return fetchable
        # try the default entry last
        if self.default_entry:
            fetchable = self.default_entry.allowance(url)
            if not fetchable:
                logger.debug('Default entry for {} not allowed.'.format(url))
            return fetchable
        # agent not found ==> access granted
        return True

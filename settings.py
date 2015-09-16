from crawler.settings import *

ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE = False
CRAWL_DEPTH = 0
CRAWL_DELAY = 1
RUN_WAIT_TIME = 300
MAX_RUN_ITERATIONS = 0
MAX_THREADS = 30
DATABASE_FILENAME = 'nlnieuws.sqlite3'

try:
    from local import *
except ImportError:
    pass
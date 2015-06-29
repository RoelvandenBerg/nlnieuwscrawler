__author__ = 'roelvdberg@gmail.com'

USER_AGENT = '_'
USER_AGENT_INFO = {
          'name' : 'python crawler',
          'organisation': '-',
          'location' : 'Unknown',
          'language' : 'Python 3'
}

VERBOSE = True
DATABASE_FILENAME = 'crawl.sqlite3'

try:
    from local import *
except ImportError:
    pass
USER_AGENT = 'Python3'
USER_AGENT_INFO = {
          'name' : 'Nieuws crawler',
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
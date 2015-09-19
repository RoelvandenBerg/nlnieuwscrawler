USER_AGENT = 'Python-urllib/3.4'
USER_AGENT_INFO = {
          'name' : 'python crawler',
          'organisation': '-',
          'location' : 'Unknown',
          'language' : 'Python 3'
}

BASE_URL = "https://www.nelen-schuurmans.nl/"
ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE = False
CRAWL_DEPTH = 1
CRAWL_DELAY = 1
RUN_WAIT_TIME = 5
MAX_RUN_ITERATIONS = 1
MAX_THREADS = 30

VERBOSE = False
DATABASE_FILENAME = 'crawl.sqlite3'
LOG_FILENAME = 'crawl.log'
RESET_DATABASE = False

NOFOLLOW = [
    "creativecommons",
    "facebook",
    "feedly",
    "flickr",
    "github",
    "google",
    "instagram",
    "last.fm",
    "linkedin",
    "mozzila",
    "openstreetmap",
    "opera",
    "sciencedirect",
    "twitter",
    "vimeo",
    "wikimedia",
    "wikipedia",
    "wiley",
    "youtube",
    'sciencecommons',
]

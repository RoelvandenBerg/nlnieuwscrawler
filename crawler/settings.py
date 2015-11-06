USER_AGENT = 'Python-urllib/3.4'
USER_AGENT_INFO = {
          'name' : 'python crawler',
          'organisation': '-',
          'location' : 'Unknown',
          'language' : 'Python 3'
}

SITES = [
    'http://www.112noordholland.nl',
    'http://www.noordhollandsdagblad.nl',
    'http://www.denoordoostpolder.nl',
    'http://www.omroepbrabant.nl',
    'http://www.omroepflevoland.nl',
    'http://www.omroepgelderland.nl',
    'http://www.omroepwest.nl',
    'http://www.omroepzeeland.nl',
    'http://www.rtvdrenthe.nl',
    'http://www.rtvnh.nl',
    'http://www.rtvoost.nl',
    'http://www.rtvutrecht.nl'
]

ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE = False
CRAWL_DEPTH = 0
CRAWL_DELAY = 5
RUN_WAIT_TIME = 5
MAX_RUN_ITERATIONS = 1
MAX_THREADS = 30

DATE_TIME_DISTANCE = 4

VERBOSE = False
DATABASE_FILENAME = 'nieuwscrawltest.sqlite3'
LOG_FILENAME = 'nieuwscrawltest.log'
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
    'sciencecommons'
]

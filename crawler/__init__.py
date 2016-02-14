__author__ = 'roelvdberg@gmail.com'

try:
    import base
    import crawl
    import filequeue
    import model
    import robot
    import validate
    import webpage
except ImportError:
    import crawler.crawl as crawl
    import crawler.base as base
    import crawler.filequeue as filequeue
    import crawler.model as model
    import crawler.robot as robot
    import crawler.validate as validate
    import crawler.webpage as webpage

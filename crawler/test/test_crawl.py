__author__ = 'roelvdberg@gmail.com'

import queue
import unittest
import unittest.mock

from crawler.crawl import *
import crawler.validate as validate


DUMMYHTML = """<html>
  <head>
    <meta charset="utf-8">
    <title>Raadselachtige knipoog van een verre ster - nrc.nl</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script>window._sf_startpt = (new Date()).getTime();</script>
    <link rel="stylesheet" href="https://static.nrc.nl/fonts/fonts.css">
  </head>
  <body>
    <div>
      <p>test</p>
    </div>
  </body>
</html>"""

LOC = urllib.parse.urljoin(SITES[-1], "test.htm")

DUMMYURLSETSITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"
        xmlns:video="http://www.google.com/schemas/sitemap-video/1.1"
        xmlns:xhtml="http://www.w3.org/1999/xhtml">
  <url>
    <loc>""" + LOC + """</loc>
    <xhtml:link rel="alternate" href="android-app://nl.sanomamedia.android.nu/http/www.nu.nl" />
    <lastmod>2015-11-02T00:08:30+01:00</lastmod>
    <changefreq>monthly</changefreq>
    <image:image>
    <image:loc>http://media.nu.nl/m/ni2x8w8a1pcd_sqr512.jpg/shorttrackers-pakken-zilver-5000-meter-relay-bij-world-cup-montreal.jpg</image:loc>
    </image:image>
    <news:news>
      <news:publication>
        <news:name>NU.nl</news:name>
        <news:language>nl</news:language>
      </news:publication>
      <news:genres>PressRelease, Blog</news:genres>
      <news:publication_date>2015-11-02T00:07:23+01:00</news:publication_date>
      <news:title>Shorttrackers pakken zilver op 5000 meter relay bij World Cup Montreal</news:title>
      <news:keywords>wereldbeker shorttrack, Sjinkie Knegt, shorttrackers</news:keywords>
    </news:news>
  </url>
</urlset>
"""

DUMMYSITEMAPINDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>http://www.test.tst/test_sitemap.xml</loc>
  </sitemap>
</sitemapindex>
"""


class HelperTestCase(unittest.TestCase):
    def test_parse_base(self):
        cases = [
            'http://test.nl',
            'http://test.nl/',
            'http://test.nl/test',
            'http://test.nl/test/',
        ]
        for case in cases:
            parsed = parse_base(case)
            self.assertEqual("test.nl", parsed)

    def test_add_url(self):
        pass

        # add_url(new_url, history, history_lock, url_queue, robot_txt)

class ValidateTestCase(unittest.TestCase):

    def setUp(self):
        self.cases = [
            '''www.nijmegennieuws.@:*.nl''',
            '''http://www.nijmegennieuws.nl''',
            '''http://www.nijmegennieuws.nl/openstreetmap''',
            '''https://www.nijmegennieuws.nl/"k'k''',
            '''http://www.nijmegennieuws.nl/test''',
            '''http://www.nijmegennieuws.nl?q=#@!$:%*()&s=<>?~="'->'''
        ]

    def test_parse_base(self):
        validated = [validate.filename(x) for x in self.cases]
        self.assertEqual(validated, ['www.nijmegennieuws.nl',
                                     'http/www.nijmegennieuws.nl',
                                     'http/www.nijmegennieuws.nl/openstreetmap',
                                     'https/www.nijmegennieuws.nl/kk',
                                     'http/www.nijmegennieuws.nl/test',
                                     'http/www.nijmegennieuws.nlq&s-'])

    def test_iri_to_uri(self):
        validated = [validate.iri_to_uri(x) for x in self.cases]
        self.assertEqual(validated, [
            'www.nijmegennieuws.@%3a*.nl',
            'http://www.nijmegennieuws.nl',
            'http://www.nijmegennieuws.nl/openstreetmap',
            'https://www.nijmegennieuws.nl/%22k%27k',
            'http://www.nijmegennieuws.nl/test',
            'http://www.nijmegennieuws.nl?q=#@!$%3a%*()&s=<>?~=%22%27->'])

    def test_url_explicit(self):
        validated = [validate.url_explicit(x) for x in self.cases]
        self.assertEqual([False, True, False, True, True, True], validated)


class MockHeaders:

    def __init__(self, calls):
        self.calls = calls

    def get_content_charset(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return 'utf-8'


class MockUrlopen:

    def __init__(self):
        self.calls = []
        self.headers = MockHeaders(self.calls)

    def read(self, *args, **kwargs):
        return DUMMYSITEMAPINDEX

    def assert_called_with(self, *args, **kwargs):
        return any(
            all(arg in called_args for arg in args) and
            all(kwarg in called_kwargs.items() for kwarg in kwargs.items())
            for called_args, called_kwargs in self.calls
        )

    def __call__(self, *args, **kwargs):
        args = tuple(x.get_full_url()
                     if isinstance(x, urllib.request.Request) else x
                     for x in args)
        kwargs = {k: v.get_full_url()
            if isinstance(v, urllib.request.Request)
            else v for k, v in kwargs.items()}
        self.calls.append((args, kwargs))
        return self

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return False


class DownloadTestCase(unittest.TestCase):

    def setUp(self):
        try:
            os.makedirs('test_data')
        except FileExistsError:
            pass
        with open('test_data/test.htm', 'w') as f:
            f.write(DUMMYHTML)
        with open('test_data/testurlset.xml', 'w') as f:
            f.write(DUMMYURLSETSITEMAP)
        with open('test_data/sitemapindex.xml', 'w') as f:
            f.write(DUMMYSITEMAPINDEX)
        with open('test_data/sitemapindex.xml', 'rb') as f_in:
            with gzip.open('test_data/sitemapindex.xml.gz', 'wb') as f_out:
                f_out.writelines(f_in)
        self.robot_txt = unittest.mock.MagicMock()
        self.robot_txt.can_fetch.return_value = True
        self.history_lock = threading.RLock()
        self.fits_xml_mock = unittest.mock.MagicMock(
            return_value=parse_xml_sitemapindex)
        self.namespace_mock = unittest.mock.MagicMock(
            return_value="{http://www.sitemaps.org/schemas/sitemap/0.9}")
        self.file_iter_mock = unittest.mock.MagicMock(
            side_effect=())

    def mock_download(self, method, *args, **kwargs):
        with unittest.mock.patch('urllib.request.urlopen', MockUrlopen()) \
                as mock_response, unittest.mock.patch(
                    'builtins.open', unittest.mock.mock_open(
                read_data="test_data/sitemapindex.xml"), create=True) as mock_file, \
                unittest.mock.patch(
                    'crawler.crawl.fits_xml', self.fits_xml_mock) as \
                        fits_xml_mock_response, \
                unittest.mock.patch(
                    'crawler.crawl.namespace', self.namespace_mock) \
                        as namespace_response, \
                unittest.mock.patch(
                    'crawler.crawl.file_iter', self.file_iter_mock) \
                        as file_iter_response:
            result = method(*args, **kwargs)
        return fits_xml_mock_response, mock_response, mock_file, result

    def test_download(self):
        _, mock_response, mock_file, result = self.mock_download(
            download_to_disk, 'http://test.tst', data_dir='test_data/data')
        self.assertTrue(mock_response.assert_called_with('http://test.tst'))
        self.assertEqual(
            str(mock_file.call_args),
            "call('test_data/data/test.tst/test.tst.crawled', 'wb')")
        self.assertEqual(result[0], "test_data/data/test.tst/test.tst.crawled")

    def test_file_iter(self):
        self.assertEqual(next(file_iter(
            "test_data/test.htm", "p", as_html=True)).text, 'test')
        self.assertEqual(next(file_iter(
            "test_data/testurlset.xml", 'loc')).text, LOC)

    def prepare(self):
        url = urllib.parse.urljoin(SITES[-1], 'test.tst')
        base = parse_base(url, http=True)
        sitemap_queue = queue.Queue()
        sitemap_queue.put('http://test.tst/test.xml')
        return pybloom.ScalableBloomFilter(), queue.Queue(), \
               url, base, sitemap_queue

    def prepare_no_sitemap(self):
        sitemap_queue = [unittest.mock.MagicMock(side_effect=filequeue.Empty)]
        return list(self.prepare()[:4]) + sitemap_queue


    def test_add_url(self):
        history, url_queue, new_url, _, _ = self.prepare()
        base = parse_base(new_url)
        add_url(base, new_url, history, self.history_lock, url_queue, self.robot_txt)
        self.assertTrue(new_url in history)
        self.assertEqual(url_queue.get(), new_url)

    def test_namespace(self):
        self.assertEqual(namespace('test_data/testurlset.xml'),
                         "{http://www.sitemaps.org/schemas/sitemap/0.9}")

    def test_fits_xml(self):
        self.assertTrue('urlset' in str(fits_xml('test_data/testurlset.xml')))

    def test_parse_xml_sitemapindex(self):
        sitemap_queue = filequeue.FileQueue()
        parse_xml_sitemapindex(path="test_data/sitemapindex.xml", sitemap_queue=sitemap_queue)
        self.assertEqual(sitemap_queue.get(), "http://www.test.tst/test_sitemap.xml")

    def test_parse_xml_urlset(self):
        history, url_queue, _, _, _ = self.prepare()
        parse_xml_urlset(base="http://www.test.tst",
                         path="test_data/testurlset.xml",
                         url_queue=url_queue,
                         history=history,
                         history_lock=self.history_lock,
                         robot_txt=self.robot_txt)
        url = url_queue.get()
        self.assertEqual(url, LOC)
        self.assertTrue(url in history)

    def test_unzip_gzip(self):
        new_file = unzip_gzip('test_data/sitemapindex.xml.gz')
        with open(new_file, 'r') as new_f:
            new_content = new_f.read()
        with open('test_data/sitemapindex.xml', 'r') as old_f:
            old_content = old_f.read()
        self.assertEqual(new_content, old_content)

    def test_download_sitemap(self):
        fits_xml_mock_response, mock_response, mock_file, result = \
            self.mock_download(
                download_sitemap, url="http://test.tst/test.xml",
                base="http://test.tst", data_dir="test_data/data")
        self.assertEqual(result, (
            'http://test.tst/test.xml', parse_xml_sitemapindex, 'test_data/data/test.tst/test.xml.crawled'))
        self.assertEqual(fits_xml_mock_response._mock_call_args[0][0], 'test_data/data/test.tst/test.xml.crawled')
        self.assertTrue(mock_file.call_args[0] == ('test_data/data/test.tst/test.xml.crawled', 'wb'))

    # def test_sitemap_worker(self):
    #     os.makedirs('test_data/data/sitemaps/test.tst')
    #     with open('test_data/data/sitemaps/test.tst/test.xml.crawled', 'w') as f:
    #         f.write('')
    #     history, url_queue, new_url, base, sitemap_queue = \
    #         self.prepare_no_sitemap()
    #     fits_xml_mock_response, mock_response, mock_file, result = \
    #         self.mock_download(
    #         sitemap_worker, base, sitemap_queue, url_queue, history,
    #         self.history_lock, self.robot_txt, data_dir='test_data/data/sitemaps')
    #     print(mock_response, mock_file, result)

    # def test_webpage_worker(self):
    #     history, url_queue, new_url, base, sitemap_queue = self.prepare()
    #     fits_xml_mock_response, mock_response, mock_file, result = \
    #         self.mock_download(
    #         webpage_worker, base, sitemap_queue, url_queue, history,
    #         self.history_lock, self.robot_txt, data_dir='test_data/data/sitemaps')
    #     print(mock_response, mock_file, result)


    def tearDown(self):
        shutil.rmtree('test_data')


if __name__ == '__main__':
    unittest.main()

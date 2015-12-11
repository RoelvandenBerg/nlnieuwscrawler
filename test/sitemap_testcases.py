__author__ = 'roelvdberg@gmail.com'

import os
import lxml
import gzip
import zipfile
import json

working_directory = os.getcwd()
root = working_directory

def make_file(before, after, start, end, filenames, location,
              other_location, encoding=False):
    if encoding:
        filenames = [encoding + '_' + f for f in filenames]
    between = ''.join(start + root + '/' + other_location + '/' + filename +
                      end for filename in filenames)
    content = before + between + after
    if encoding:
        content.encode(encoding)
    with open(working_directory + location, 'wb' if encoding else 'w') as f:
        f.write(content)


sitemapindex_before = '''
<?xml version="1.0" encoding="UTF-8"?>

<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
'''
sitemapindex_start = '''
  <sitemap>
    <loc>'''
sitemapindex_end = '''</loc>
  </sitemap>
'''
sitemapindex_after = '''
</sitemapindex>
'''

sitemap1_before = '''
<?xml version="1.0" encoding="UTF-8"?>

<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
'''
sitemap1_start = '''
  <url>
    <loc>'''
sitemap1_end = '''</loc>
    <lastmod>2015-07-25T18:10:32+00:00</lastmod>
  </url>
'''
sitemap1_after = '''
</urlset>
'''

xml_filenames = ['1', '2.xml']
html_filenames = ['vk', 'nrc.htm', 'trouw.html', 'telegraaf', 'bndestem']

from test.webpage_testcases import *


try:
    os.mkdir(working_directory + '/sitemap')
    os.mkdir(working_directory + '/site')
except FileExistsError:
    pass

make_file(sitemapindex_before, sitemapindex_after, sitemapindex_start,
          sitemapindex_end, xml_filenames, '/sitemap/sitemapindex', 'sitemap')

for filename in xml_filenames:
    make_file(sitemap1_before, sitemap1_after, sitemap1_start, sitemap1_end,
          html_filenames, '/sitemap/' + filename, 'site')

for i, filename in enumerate(html_filenames):
    make_file('', '', '', papers[i],
          [''], '/site/' + filename, '')

with open('sitemapindexlocation.txt', 'w') as f:
    f.write(root + '/sitemap/sitemapindex')




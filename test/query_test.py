# -*- coding:utf-8 -*-

import sys

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

s = XapianSearch(path='../data/', name='statuses')

results = s.search(query={'text': [u'中国'], 'uid': 1217743083, 'ts': {'$gt': 0, '$lt': 1334450340}}, sort_by=['-ts'], fields=['text', 'ts'])

for r in results['results']:
    print r

print 'hits: %s' % results['hits']


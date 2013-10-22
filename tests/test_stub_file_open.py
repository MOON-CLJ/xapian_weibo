# -*- coding:utf-8 -*-

import sys
import os

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch
from utils import top_keywords, not_low_freq_keywords

"""
stub = '/home/arthas/dev/xapian_weibo/stub/master_timeline_weibo_20130929'
s = XapianSearch(stub=stub)
count, get_results = s.search(query={'text': [u'中国']}, sort_by=['-timestamp'], fields=['text', 'timestamp', 'user', 'terms', '_id'])

print 'query1:'

for r in get_results():
    print "** " * 10
    print r['_id']
    print r['user']
    print r['text']
    print r['timestamp']
    print r['terms']

print 'hits: %s' % count

stub = '/home/arthas/dev/xapian_weibo/stub/master_timeline_weibo_20130929'
s = XapianSearch(stub=stub, include_remote=True)
count, get_results = s.search(query={'text': [u'中国']}, sort_by=['-timestamp'], fields=['text', 'timestamp', 'user', 'terms', '_id'])

print 'query2:'

for r in get_results():
    print "** " * 10
    print r['_id']
    print r['user']
    print r['text']
    print r['timestamp']
    print r['terms']

print 'hits: %s' % count
"""

print "query3:"

stub = '/home/arthas/dev/xapian_weibo/stub/master_timeline_weibo_20130929'
s = XapianSearch(stub=stub, include_remote=True)
results = s.iter_all_docs()
count = 0
for r in results:
    count += 1
print 'hits: ', count

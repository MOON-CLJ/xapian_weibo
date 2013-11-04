# -*- coding:utf-8 -*-

import sys
import time
import datetime

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

"""
print "query1:"

stub = '/home/mirage/clj/dev/xapian_weibo/tests/master_timeline_weibo_20131104'
s = XapianSearch(stub=stub, include_remote=True)
results = s.iter_all_docs()
print 'hehe'
count = 0
te = ts = time.time()
for r in results:
    count += 1
    if count % 10000 == 0:
        te = time.time()
        print te - ts
        ts = te
print 'hits: ', count
"""

print "query2:"

stub = '/home/mirage/clj/dev/xapian_weibo/tests/master_timeline_weibo_20131104'
s = XapianSearch(stub=stub, include_remote=True)
begin_ts1 = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
end_ts1 = time.mktime(datetime.datetime(2013, 1, 15).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': end_ts1},
    '$not': {'retweeted_status': '0'}
}
count, get_results = s.search(query=query_dict, fields=['user'])
print 'hints', count

count = 0
te = ts = time.time()
for r in get_results():
    count += 1
    if count % 100000 == 0:
        te = time.time()
        print te - ts
        ts = te

# -*- coding:utf-8 -*-

import sys
import time

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

print "query1:"

stub = '/home/mirage/clj/dev/xapian_weibo/tests/master_timeline_weibo_20131104'
s = XapianSearch(stub=stub, include_remote=True)
results = s.iter_all_docs()
count = 0
te = ts = time.time()
for r in results:
    count += 1
    if count % 10000 == 0:
        te = time.time()
        print te - ts
        ts = te
print 'hits: ', count

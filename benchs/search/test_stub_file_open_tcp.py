# -*- coding:utf-8 -*-

import sys
import time
import datetime

from xapian_weibo.xapian_backend import XapianSearch

stub = 'master_timeline_weibo_stub'
s = XapianSearch(stub=stub)
begin_ts1 = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
end_ts1 = time.mktime(datetime.datetime(2013, 1, 15).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': end_ts1},
    '$not': {'retweeted_status': '0'}
}
_, get_results = s.search(query=query_dict, fields=['user', 'terms'])

for r in get_results():
    pass

# -*- coding:utf-8 -*-

import time
import datetime

from xapian_weibo.xapian_backend import XapianSearch

s = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

begin_ts = time.mktime(datetime.datetime(2011, 1, 1).timetuple())
end_ts = time.mktime(datetime.datetime(2011, 12, 31).timetuple())


"""
query_dict = {
    'created_at': {
        '$gt': begin_ts,
        '$lt': end_ts,
    }
}
count, get_results = s.search(query=query_dict, max_offset=1, fields=['_id', 'name'], sort_by=['created_at'])

print count
for r in get_results():
    print r['_id'], r['name']
"""

"""
query_dict = {
    '$or': [
        {'_id': 1934744637},
        {'_id': 1908252575},
    ]
}
"""
query_dict = {'_id': 1908252575}

count, get_results = s.search(query=query_dict, fields=['_id', 'name'])

print count
for r in get_results():
    print r['_id'], r['name']

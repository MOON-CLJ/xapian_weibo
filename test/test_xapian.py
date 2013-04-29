# -*- coding:utf-8 -*-

import time
import datetime
from xapian_weibo.xapian_backend import XapianSearch

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


@timeit
def load_weibos_from_xapian():
    begin_ts = time.mktime(datetime.datetime(2012, 1, 1).timetuple())
    end_ts = time.mktime(datetime.datetime(2013, 3, 1).timetuple())

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }
    count, get_results = s.search(query=query_dict, fields=['id', 'retweeted_status', 'text'])
    print count
    return get_results


@timeit
def test_xapian_read(get_results, n):
    count = 0
    for r in get_results():
        count += 1
        if count == n:
            break

if __name__ == '__main__':
    get_results = load_weibos_from_xapian()
    test_xapian_read(get_results, 10000)
    test_xapian_read(get_results, 100000)

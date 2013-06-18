# -*- coding: utf-8 -*-

import hurdles
import datetime
import time
from hurdles.tools import extra_setup
from xapian_weibo.xapian_backend import XapianSearch


class BenchXapianR(hurdles.BenchCase):
    def setUp(self):
        self.weibo_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_weibo')
        self.user_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
        self.begin_ts = time.mktime(datetime.datetime(2011, 12, 1).timetuple())
        self.end_ts = time.mktime(datetime.datetime(2011, 12, 31).timetuple())

    def tearDown(self):
        pass

    """
    def bench_load_users(self):
        query_dict = {
            'created_at': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        count, get_results = self.user_x.search(query=query_dict, fields=['_id', 'name'])
        print count

    def bench_load_users_then_sort(self):
        query_dict = {
            'created_at': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        count, get_results = self.user_x.search(query=query_dict, fields=['_id', 'name'], sort_by=['created_at'])
        print count

    def bench_load_weibos(self):
        query_dict = {
            'timestamp': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        count, get_results = self.weibo_x.search(query=query_dict, fields=['_id', 'user'])
        print count
    """

    def bench_get_results_weibos(self):
        query_dict = {
            'timestamp': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        _, get_results = self.weibo_x.search(query=query_dict, fields=['_id', 'user'])
        for r in get_results():
            _id = r['_id']

    def bench_get_results_users(self, *args, **kwargs):
        query_dict = {
            'created_at': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        _, get_results = self.user_x.search(query=query_dict, fields=['_id', 'name'])
        for r in get_results():
            _id = r['_id']

    """
    hurdles bench_xapian_r.py
    204909
    BenchXapianR.bench_load_users
     | average       3089.497 ms
     | median        3096.1 ms
     | fastest       3023.16 ms
     | slowest       3110.57 ms
    204909
    BenchXapianR.bench_load_users_then_sort
     | average       3482.546 ms
     | median        3493.0 ms
     | fastest       3370.14 ms
     | slowest       3512.91 ms
    BenchXapianR.bench_get_results_users
     | average       10772.205 ms
     | median        10772.55 ms
     | fastest       10733.5 ms
     | slowest       10833.13 ms
    BenchXapianR.bench_get_results_weibos
     | average       548270.836 ms
     | median        381635.815 ms
     | fastest       379506.34 ms
     | slowest       1186662.3 ms
    8159825
    BenchXapianR.bench_load_weibos
     | average       111222.65 ms
     | median        110983.14 ms
     | fastest       104205.26 ms
     | slowest       119472.44 ms

    ------------------------------------------------------------
    Ran 5 benchmarks
    """

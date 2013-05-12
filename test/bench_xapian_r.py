# -*- coding: utf-8 -*-

import hurdles
import datetime
import time
from xapian_weibo.xapian_backend import XapianSearch


class BenchXapianR(hurdles.BenchCase):
    def setUp(self):
        self.weibo_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_weibo')
        self.user_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_user')
        self.begin_ts = time.mktime(datetime.datetime(2011, 12, 1).timetuple())
        self.end_ts = time.mktime(datetime.datetime(2011, 12, 31).timetuple())

    def tearDown(self):
        pass

    def bench_load_users(self):
        query_dict = {
            'created_at': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        count, get_results = self.user_x.search(query=query_dict, fields=['_id', 'name'])

    def bench_load_users_then_sort(self):
        query_dict = {
            'created_at': {
                '$gt': self.begin_ts,
                '$lt': self.end_ts,
            }
        }
        count, get_results = self.user_x.search(query=query_dict, fields=['_id', 'name'], sort_by=['created_at'])

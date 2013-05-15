# -*- coding: utf-8 -*-

import hurdles
import time
import datetime
from xapian_weibo.xapian_backend import XapianSearch


class BenchXapianGetByIds(hurdles.BenchCase):
    def setUp(self):
        self.n = 10000
        self.s = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_weibo')
        self.weibo_ids = self._load_weibo_ids_from_xapian(self.n)

    def tearDown(self):
        pass

    def _load_weibo_ids_from_xapian(self, limit):
        begin_ts = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
        end_ts = time.mktime(datetime.datetime(2013, 1, 2).timetuple())

        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        }
        count, get_results = self.s.search(query=query_dict, max_offset=limit, fields=['_id'])
        print count
        ids = []
        for r in get_results():
            ids.append(r['_id'])

        return ids

    def bench_1(self):
        for _id in self.weibo_ids:
            query_dict = {'_id': _id}
            count, get_results = self.s.search(query=query_dict, fields=['_id', 'text'])

    def bench_10(self):
        size = 10
        for i in xrange(self.n / size):
            query_dict = {
                '$or': [],
            }

            for _id in self.weibo_ids[i * size: (i + 1) * size]:
                query_dict['$or'].append({'_id': _id})

            count, get_results = self.s.search(query=query_dict, fields=['_id', 'text'])

    def bench_20(self):
        size = 20
        for i in xrange(self.n / size):
            query_dict = {
                '$or': [],
            }

            for _id in self.weibo_ids[i * size: (i + 1) * size]:
                query_dict['$or'].append({'_id': _id})

            count, get_results = self.s.search(query=query_dict, fields=['_id', 'text'])

    def bench_30(self):
        size = 30
        for i in xrange(self.n / size):
            query_dict = {
                '$or': [],
            }

            for _id in self.weibo_ids[i * size: (i + 1) * size]:
                query_dict['$or'].append({'_id': _id})

            count, get_results = self.s.search(query=query_dict, fields=['_id', 'text'])

    def bench_50(self):
        size = 50
        for i in xrange(self.n / size):
            query_dict = {
                '$or': [],
            }

            for _id in self.weibo_ids[i * size: (i + 1) * size]:
                query_dict['$or'].append({'_id': _id})

            count, get_results = self.s.search(query=query_dict, fields=['_id', 'text'])

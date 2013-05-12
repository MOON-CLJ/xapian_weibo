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

    @extra_setup("import datetime\n"
                 "import time\n"
                 "from xapian_weibo.xapian_backend import XapianSearch\n"
                 "weibo_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_weibo')\n"
                 "begin_ts = time.mktime(datetime.datetime(2011, 12, 1).timetuple())\n"
                 "end_ts = time.mktime(datetime.datetime(2011, 12, 31).timetuple())\n"
                 "query_dict = {\n"
                 "    'timestamp': {\n"
                 "        '$gt': begin_ts,\n"
                 "        '$lt': end_ts,\n"
                 "    }\n"
                 "}\n"
                 "count, get_results = weibo_x.search(query=query_dict, max_offset=1000000, fields=['_id', 'user'])")
    def bench_get_results_weibos(self, *args, **kwargs):
        print kwargs['count']
        for r in kwargs['get_results']():
            _id = r['_id']

    @extra_setup("import datetime\n"
                 "import time\n"
                 "from xapian_weibo.xapian_backend import XapianSearch\n"
                 "user_x = XapianSearch(path='/home/arthas/dev/xapian_weibo/data/', name='master_timeline_user', schema_version=1)\n"
                 "begin_ts = time.mktime(datetime.datetime(2011, 12, 1).timetuple())\n"
                 "end_ts = time.mktime(datetime.datetime(2011, 12, 31).timetuple())\n"
                 "query_dict = {\n"
                 "    'created_at': {\n"
                 "        '$gt': begin_ts,\n"
                 "        '$lt': end_ts,\n"
                 "    }\n"
                 "}\n"
                 "count, get_results = user_x.search(query=query_dict, max_offset=1000000, fields=['_id', 'name'])")
    def bench_get_results_users(self, *args, **kwargs):
        print kwargs['count']
        for r in kwargs['get_results']():
            _id = r['_id']

    """
    hurdles bench_xapian_r.py
    172380
    BenchXapianR.bench_get_results_users
     | average   13148.946 ms
     | median    6056.98 ms
     | fastest   6051.95 ms
     | slowest   74002.44 ms
    7728057
    BenchXapianR.bench_get_results_weibos
     | average   38832.484 ms
     | median    30021.775 ms
     | fastest   29851.23 ms
     | slowest   109202.47 ms
    172380
    BenchXapianR.bench_load_users
     | average   5372.263 ms
     | median    5383.485 ms
     | fastest   5236.07 ms
     | slowest   5404.19 ms
    172380
    BenchXapianR.bench_load_users_then_sort
     | average   6116.02 ms
     | median    6138.785 ms
     | fastest   5896.72 ms
     | slowest   6149.85 ms
    7728057
    BenchXapianR.bench_load_weibos
     | average   225169.826 ms
     | median    225803.705 ms
     | fastest   217562.72 ms
     | slowest   227734.99 ms

    ------------------------------------------------------------
    Ran 5 benchmarks
    """

# -*- coding: utf-8 -*-

import hurdles
import datetime
import time
import pycassa
from hurdles.tools import extra_setup
from xapian_weibo.bs_input import KeyValueBSONInput
from pycassa.pool import ConnectionPool

BSON_FILEPATH = '/home/arthas/mongodumps/20130510/master_timeline/master_timeline_weibo.bson'


def load_items(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchCassandraW(hurdles.BenchCase):
    def setUp(self):
        n = 100000
        self.weibo_ids = self._load_items(n)
        pool = ConnectionPool('master_timeline')
        col_fam = pycassa.ColumnFamily(pool, 'weibos')
        self.weibos_col_fam = col_fam

    def tearDown(self):
        pass

    def _load_items(self, limit):
        weibo_ids = []
        bs_input = load_items()
        count = 0
        for _, item in bs_input.reads():
            weibo_ids.append(item['_id'])
            count += 1
            if count == limit:
                break

        return weibo_ids

    def bench_get(self):
        for _id in self.weibo_ids:
            self.weibos_col_fam.get(_id)

    def bench_batch_get_100(self):
        batch_size = 100
        for i in xrange(len(self.weibo_ids) / batch_size):
            weibo_ids = []
            for _id in self.weibo_ids[i * batch_size: (i + 1) * batch_size]:
                weibo_ids.append(_id)
            self.weibos_col_fam.multiget(weibo_ids)

    def bench_batch_get_1000(self):
        batch_size = 1000
        for i in xrange(len(self.weibo_ids) / batch_size):
            weibo_ids = []
            for _id in self.weibo_ids[i * batch_size: (i + 1) * batch_size]:
                weibo_ids.append(_id)
            self.weibos_col_fam.multiget(weibo_ids)

    def bench_batch_get_10000(self):
        batch_size = 10000
        for i in xrange(len(self.weibo_ids) / batch_size):
            weibo_ids = []
            for _id in self.weibo_ids[i * batch_size: (i + 1) * batch_size]:
                weibo_ids.append(_id)
            self.weibos_col_fam.multiget(weibo_ids)

    def bench_get_text_column(self):
        for _id in self.weibo_ids:
            self.weibos_col_fam.get(_id, columns=['text'])

    def bench_get_text_else_column(self):
        for _id in self.weibo_ids:
            self.weibos_col_fam.get(_id, columns=['timestamp', 'reposts_count'])

    """
    1 node:
    hurdles bench_cassandra_r.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_get_100
     | average       13791.493 ms
     | median        13783.21 ms
     | fastest       13588.74 ms
     | slowest       14122.8 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_get_1000
     | average       10067.342 ms
     | median        10078.3 ms
     | fastest       9993.99 ms
     | slowest       10134.61 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_get_10000
     | average       11153.9 ms
     | median        11128.81 ms
     | fastest       10900.68 ms
     | slowest       11397.4 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_get
     | average       108401.059 ms
     | median        108689.275 ms
     | fastest       106764.0 ms
     | slowest       109751.19 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_get_text_column
     | average       43630.959 ms
     | median        43474.53 ms
     | fastest       42061.52 ms
     | slowest       46685.7 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_get_text_else_column
     | average       46165.727 ms
     | median        46162.76 ms
     | fastest       45903.92 ms
     | slowest       46653.55 ms
    """

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
        self.weibos = self._load_items(n)
        pool = ConnectionPool('master_timeline')
        col_fam = pycassa.ColumnFamily(pool, 'weibos')
        self.weibos_col_fam = col_fam

    def tearDown(self):
        pass

    def _load_items(self, limit):
        weibos = []
        bs_input = load_items()
        count = 0
        for _, item in bs_input.reads():
            weibos.append(item)
            count += 1
            if count == limit:
                break

        return weibos

    def bench_insert(self):
        for weibo in self.weibos:
            self.weibos_col_fam.insert(weibo['_id'], {
                'text': weibo['text'],
                'timestamp': weibo['timestamp'],
                'reposts_count': weibo['reposts_count']
            })

    def bench_batch_insert_100(self):
        batch_size = 100
        for i in xrange(len(self.weibos) / batch_size):
            weibos_data = {}
            for weibo in self.weibos[i * batch_size: (i + 1) * batch_size]:
                weibos_data[weibo['_id']] = {
                    'text': weibo['text'],
                    'timestamp': weibo['timestamp'],
                    'reposts_count': weibo['reposts_count']
                }
            self.weibos_col_fam.batch_insert(weibos_data)

    def bench_batch_insert_1000(self):
        batch_size = 1000
        for i in xrange(len(self.weibos) / batch_size):
            weibos_data = {}
            for weibo in self.weibos[i * batch_size: (i + 1) * batch_size]:
                weibos_data[weibo['_id']] = {
                    'text': weibo['text'],
                    'timestamp': weibo['timestamp'],
                    'reposts_count': weibo['reposts_count']
                }
            self.weibos_col_fam.batch_insert(weibos_data)

    def bench_batch_insert_10000(self):
        batch_size = 10000
        for i in xrange(len(self.weibos) / batch_size):
            weibos_data = {}
            for weibo in self.weibos[i * batch_size: (i + 1) * batch_size]:
                weibos_data[weibo['_id']] = {
                    'text': weibo['text'],
                    'timestamp': weibo['timestamp'],
                    'reposts_count': weibo['reposts_count']
                }
            self.weibos_col_fam.batch_insert(weibos_data)

    """
    hurdles bench_cassandra_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_100
     | average       12110.793 ms
     | median        12031.805 ms
     | fastest       11717.56 ms
     | slowest       12872.64 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_1000
     | average       11013.973 ms
     | median        11293.05 ms
     | fastest       9748.5 ms
     | slowest       12342.8 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_10000
     | average       11047.858 ms
     | median        10829.465 ms
     | fastest       10628.46 ms
     | slowest       12941.17 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_insert
     | average       47498.512 ms
     | median        47691.89 ms
     | fastest       45528.44 ms
     | slowest       48641.06 ms
    """

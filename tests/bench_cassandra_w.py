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
        n = 10000
        self.weibos = self._load_items(n)
        pool = ConnectionPool('master_timeline', server_list=['219.224.135.60:9160', '219.224.135.61:9160'], pool_size=10)
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

    def bench_batch_insert(self):
        weibos_data = {}
        for weibo in self.weibos:
            weibos_data[weibo['_id']] = {
                'text': weibo['text'],
                'timestamp': weibo['timestamp'],
                'reposts_count': weibo['reposts_count']
            }
        self.weibos_col_fam.batch_insert(weibos_data)

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

    """
    schema:
    create keyspace master_timeline
    with placement_strategy = 'SimpleStrategy'
    and strategy_options = {replication_factor : 1};

    create column family weibos
      with comparator = 'UTF8Type'
      and default_validation_class = 'UTF8Type'
      and key_validation_class = 'LongType'
      and column_metadata = [
        {column_name : 'reposts_count', validation_class : Int32Type},
        {column_name : 'timestamp', validation_class : LongType},
        {column_name : 'text',validation_class : UTF8Type}];

    1 node:
    hurdles bench_cassandra_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert
     | average       922.751 ms
     | median        917.325 ms
     | fastest       527.56 ms
     | slowest       1396.64 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_100
     | average       1057.972 ms
     | median        853.685 ms
     | fastest       714.03 ms
     | slowest       2828.66 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_1000
     | average       856.124 ms
     | median        861.965 ms
     | fastest       792.65 ms
     | slowest       873.46 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_insert
     | average       4721.272 ms
     | median        4684.31 ms
     | fastest       4518.87 ms
     | slowest       4994.72 ms

    2 node:
    hurdles bench_cassandra_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert
     | average       1341.845 ms
     | median        1087.74 ms
     | fastest       853.52 ms
     | slowest       4060.94 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_100
     | average       1384.371 ms
     | median        1280.365 ms
     | fastest       1047.3 ms
     | slowest       2311.91 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_batch_insert_1000
     | average       1140.565 ms
     | median        1153.18 ms
     | fastest       1030.62 ms
     | slowest       1174.49 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchCassandraW.bench_insert
     | average       8558.226 ms
     | median        8501.005 ms
     | fastest       8270.73 ms
     | slowest       9316.03 ms
    """

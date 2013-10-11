# -*- coding: utf-8 -*-

import hurdles
import MySQLdb
from xapian_weibo.bs_input import KeyValueBSONInput
from xapian_weibo.xapian_backend import Schema

BSON_FILEPATH = '/home/arthas/mongodumps/20130510/master_timeline/master_timeline_weibo.bson'

iter_keys = Schema.v2['origin_data_iter_keys']
iter_keys.remove('geo')

def load_items(bs_filepath=BSON_FILEPATH):
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchBdbRealWeiboDataR(hurdles.BenchCase):
    def setUp(self):
        n = 100000
        self.n = n
        self.weibo_ids = self._load_items(n)

        conn = MySQLdb.connect(user='root', passwd='', db='master_timeline')
        self.conn = conn
        self.cursor = conn.cursor()

    def tearDown(self):
        self.conn.close()

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

    """
    def bench_get_whole_1(self):
        sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id=%s'
        for _id in self.weibo_ids:
            self.cursor.execute(sql % _id)
            r = self.cursor.fetchone()

    def bench_get_whole_1000(self):
        size = 1000
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()
    """

    def bench_get_whole_10000(self):
        size = 10000
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()

    """
    def bench_get_whole_20000(self):
        size = 20000
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()

    def bench_get_whole_100000(self):
        size = 100000
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()
    """

    def bench_get_whole_10000_text(self):
        size = 10000
        iter_keys = ['text']
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()

    def bench_get_whole_10000_text_timestamp(self):
        size = 10000
        iter_keys = ['text', 'timestamp']
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()

    def bench_get_whole_10000_text_timestamp_user(self):
        size = 10000
        iter_keys = ['text', 'timestamp', 'user']
        for i in xrange(self.n / size):
            ids = [str(_id) for _id in self.weibo_ids[i * size: (i + 1) * size]]
            sql = 'select ' + ','.join(iter_keys) + ' from master_timeline_weibo where _id in (%s);' % ','.join(ids)
            self.cursor.execute(sql)
            self.cursor.fetchall()

    """
    BenchBdbRealWeiboDataR.bench_get_whole_1
     | average       28089.308 ms
     | median        23709.53 ms
     | fastest       23326.47 ms
     | slowest       67805.1 ms
    BenchBdbRealWeiboDataR.bench_get_whole_1000
     | average       1984.215 ms
     | median        1983.73 ms
     | fastest       1970.25 ms
     | slowest       1998.21 ms
    BenchBdbRealWeiboDataR.bench_get_whole_10000
     | average       1678.545 ms
     | median        1674.915 ms
     | fastest       1667.33 ms
     | slowest       1710.02 ms
    BenchBdbRealWeiboDataR.bench_get_whole_100000
     | average       1715.538 ms
     | median        1723.515 ms
     | fastest       1679.19 ms
     | slowest       1758.09 ms
    BenchBdbRealWeiboDataR.bench_get_whole_20000
     | average       1694.948 ms
     | median        1694.615 ms
     | fastest       1687.34 ms
     | slowest       1702.63 ms
    BenchBdbRealWeiboDataR.bench_get_whole_10000_text
     | average       1241.727 ms
     | median        1247.025 ms
     | fastest       1205.63 ms
     | slowest       1250.91 ms
    BenchBdbRealWeiboDataR.bench_get_whole_10000_text_timestamp
     | average       1295.397 ms
     | median        1291.68 ms
     | fastest       1288.23 ms
     | slowest       1312.02 ms
    BenchBdbRealWeiboDataR.bench_get_whole_10000_text_timestamp_user
     | average       1393.484 ms
     | median        1391.975 ms
     | fastest       1388.78 ms
     | slowest       1404.53 ms
    """

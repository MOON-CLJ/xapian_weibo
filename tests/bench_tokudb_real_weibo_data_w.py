# -*- coding: utf-8 -*-

import msgpack
import MySQLdb
from xapian_weibo.bs_input import KeyValueBSONInput
from xapian_weibo.utils import timeit
from xapian_weibo.xapian_backend import Schema

BSON_FILEPATH = '/home/arthas/mongodumps/20130510/master_timeline/master_timeline_weibo.bson'

iter_keys = Schema.v2['origin_data_iter_keys']
pre_func = Schema.v2['pre_func']

def load_items(bs_filepath=BSON_FILEPATH):
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchTokudbRealWeiboDataW():
    def setUp(self):
        n = 5000000
        self.weibos = self._load_items(n)

        conn = MySQLdb.connect(user='root', passwd='', db='master_timeline')
        self.conn = conn
        self.cursor = conn.cursor()

    def tearDown(self):
        self.cursor.execute('delete from master_timeline_weibo')
        self.conn.commit()
        self.conn.close()

    def _load_items(self, limit):
        weibos = []
        bs_input = load_items()
        count = 0
        for _, item in bs_input.reads():
            item = dict([(k, pre_func[k](item.get(k)) if k in pre_func else item.get(k)) for k in iter_keys])
            for k, v in item.iteritems():
                if k in ['text', 'source', 'bmiddle_pic'] and isinstance(v, unicode):
                    item[k] = v.encode('utf8')
            weibos.append(item)
            count += 1
            if count == limit:
                break

        return weibos

    @timeit
    def bench_single_write(self):
        for weibo in self.weibos:
            self.cursor.execute(
                """insert into master_timeline_weibo (_id, user, retweeted_status, text, timestamp, reposts_count, source, bmiddle_pic, geo)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [weibo[i] for i in iter_keys]
            )
        self.conn.commit()

    @timeit
    def bench_batch_write(self):
        size = 10000
        count = 0
        weibos = []
        for weibo in self.weibos:
            weibos.append([weibo[i] for i in iter_keys])
            count += 1

            if count == size:
                self.cursor.executemany(
                    """insert into master_timeline_weibo (_id, user, retweeted_status, text, timestamp, reposts_count, source, bmiddle_pic, geo)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    weibos
                )
                self.conn.commit()
                weibos = []
                count = 0


if __name__ == '__main__':
    bench = BenchTokudbRealWeiboDataW()

    for i in range(1):
        bench.setUp()
        bench.bench_single_write()
        bench.tearDown()

    for i in range(2):
        bench.setUp()
        bench.bench_batch_write()
        bench.tearDown()

    """
    n 100000
    'bench_single_write' 31.93 sec
    'bench_batch_write' 12.72 sec
    n 5000000
    'bench_batch_write' 887.90 sec
    """

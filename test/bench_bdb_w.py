# -*- coding: utf-8 -*-

import hurdles
import pycassa
import bsddb3
import msgpack
from bsddb3 import db
from hurdles.tools import extra_setup
from xapian_weibo.bs_input import KeyValueBSONInput

BSON_FILEPATH = '/home/arthas/mongodumps/20130510/master_timeline/master_timeline_weibo.bson'
BDB_DATA_PATH = '/home/arthas/berkeley/data'
BDB_LOG_PATH = '/home/arthas/berkeley/log'
BDB_TMP_PATH = '/home/arthas/berkeley/tmp'


def load_items(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchBdbW(hurdles.BenchCase):
    def setUp(self):
        n = 100000
        self.weibos = self._load_items(n)
        self.db_env = db.DBEnv()
        self.db_env.set_tmp_dir(BDB_TMP_PATH)
        self.db_env.set_lg_dir(BDB_LOG_PATH)
        self.db_env.set_cachesize(0, 8 * (2 << 25), 1)
        self.db_env.open(BDB_DATA_PATH, db.DB_INIT_CDB | db.DB_INIT_MPOOL | db.DB_CREATE)

        weibo_hash_db = db.DB(self.db_env)
        weibo_hash_db.open('weibo_hash', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db = weibo_hash_db

        weibo_btree_db = db.DB(self.db_env)
        weibo_btree_db.open('weibo_btree', None, db.DB_BTREE, db.DB_CREATE)
        self.weibo_btree_db = weibo_btree_db

    def tearDown(self):
        self.weibo_hash_db.close()
        self.weibo_btree_db.close()

        self.db_env.close()

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

    def bench_insert_hash(self):
        for weibo in self.weibos:
            self.weibo_hash_db.put(str(weibo['_id']), msgpack.packb({
                'text': weibo['text'],
                'timestamp': weibo['timestamp'],
                'reposts_count': weibo['reposts_count']
            }))

    def bench_insert_btree(self):
        for weibo in self.weibos:
            self.weibo_btree_db.put(str(weibo['_id']), msgpack.packb({
                'text': weibo['text'],
                'timestamp': weibo['timestamp'],
                'reposts_count': weibo['reposts_count']
            }))

    """
    hurdles bench_bdb_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_btree
     | average       1479.397 ms
     | median        1474.745 ms
     | fastest       1472.57 ms
     | slowest       1500.82 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_hash
     | average       2022.588 ms
     | median        1991.535 ms
     | fastest       1970.11 ms
     | slowest       2268.33 ms

    set_cachesize(0, 8 * (2 << 25), 1)
    hurdles bench_bdb_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_btree
     | average       1224.858 ms
     | median        1213.94 ms
     | fastest       1213.38 ms
     | slowest       1314.3 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_hash
     | average       2075.785 ms
     | median        1134.595 ms
     | fastest       1128.01 ms
     | slowest       10331.8 ms

    1000000: set_cachesize(0, 8 * (2 << 25), 1)
    hurdles bench_bdb_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_btree
     | average       15746.9 ms
     | median        12465.225 ms
     | fastest       12288.77 ms
     | slowest       37078.99 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_insert_hash
     | average       23262.231 ms
     | median        13217.18 ms
     | fastest       13018.73 ms
     | slowest       107714.15 ms
    """

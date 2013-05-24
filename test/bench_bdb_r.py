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
        self.weibo_ids = self._load_items(n)
        self.db_env = db.DBEnv()
        self.db_env.set_tmp_dir(BDB_TMP_PATH)
        self.db_env.set_lg_dir(BDB_LOG_PATH)
        self.db_env.set_cachesize(0, 8 * (2 << 25), 1)
        self.db_env.open(BDB_DATA_PATH, db.DB_INIT_CDB | db.DB_INIT_MPOOL)

        weibo_hash_db = db.DB(self.db_env)
        weibo_hash_db.open('weibo_hash', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db = weibo_hash_db

        weibo_hash_db_rdonly = db.DB(self.db_env)
        weibo_hash_db_rdonly.open('weibo_hash', None, db.DB_HASH, db.DB_RDONLY)
        self.weibo_hash_db_rdonly = weibo_hash_db_rdonly

        weibo_btree_db = db.DB(self.db_env)
        weibo_btree_db.open('weibo_btree', None, db.DB_BTREE, db.DB_CREATE)
        self.weibo_btree_db = weibo_btree_db

        weibo_btree_db_rdonly = db.DB(self.db_env)
        weibo_btree_db_rdonly.open('weibo_btree', None, db.DB_BTREE, db.DB_RDONLY)
        self.weibo_btree_db_rdonly = weibo_btree_db_rdonly

    def tearDown(self):
        self.weibo_hash_db.close()
        self.weibo_btree_db.close()
        self.weibo_hash_db_rdonly.close()
        self.weibo_btree_db_rdonly.close()

        self.db_env.close()

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

    def bench_get_hash(self):
        for _id in self.weibo_ids:
            msgpack.unpackb(self.weibo_hash_db.get(str(_id)))

    def bench_get_btree(self):
        for _id in self.weibo_ids:
            msgpack.unpackb(self.weibo_btree_db.get(str(_id)))

    def bench_get_hash_rdonly(self):
        for _id in self.weibo_ids:
            msgpack.unpackb(self.weibo_hash_db_rdonly.get(str(_id)))

    def bench_get_btree_rdonly(self):
        for _id in self.weibo_ids:
            msgpack.unpackb(self.weibo_btree_db_rdonly.get(str(_id)))

    """
    hurdles bench_bdb_r.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree
     | average       646.768 ms
     | median        646.505 ms
     | fastest       644.65 ms
     | slowest       650.26 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree_rdonly
     | average       649.516 ms
     | median        649.205 ms
     | fastest       648.29 ms
     | slowest       652.65 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash
     | average       800.82 ms
     | median        800.89 ms
     | fastest       799.29 ms
     | slowest       802.2 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash_rdonly
     | average       893.947 ms
     | median        805.155 ms
     | fastest       801.75 ms
     | slowest       1674.26 ms

    set_cachesize(0, 8 * (2 << 25), 1)
    hurdles bench_bdb_r.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree
     | average       564.431 ms
     | median        561.675 ms
     | fastest       560.39 ms
     | slowest       586.22 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree_rdonly
     | average       571.827 ms
     | median        569.495 ms
     | fastest       567.28 ms
     | slowest       595.54 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash
     | average       479.224 ms
     | median        475.51 ms
     | fastest       474.49 ms
     | slowest       507.87 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash_rdonly
     | average       477.615 ms
     | median        474.705 ms
     | fastest       473.68 ms
     | slowest       504.52 ms

    1000000: set_cachesize(0, 8 * (2 << 25), 1)
    hurdles bench_bdb_r.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree
     | average       8954.67 ms
     | median        6301.815 ms
     | fastest       6254.74 ms
     | slowest       25048.63 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_btree_rdonly
     | average       7073.685 ms
     | median        6362.565 ms
     | fastest       6240.02 ms
     | slowest       11888.71 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash
     | average       6120.271 ms
     | median        5677.795 ms
     | fastest       5481.62 ms
     | slowest       8484.88 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbW.bench_get_hash_rdonly
     | average       6351.83 ms
     | median        5664.685 ms
     | fastest       5626.03 ms
     | slowest       10772.89 ms
    """

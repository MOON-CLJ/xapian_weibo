# -*- coding: utf-8 -*-

import hurdles
import bsddb3
import msgpack
from bsddb3 import db
from xapian_weibo.bs_input import KeyValueBSONInput

BSON_FILEPATH = '/home/arthas/mongodumps/20130510/master_timeline/master_timeline_weibo.bson'
BDB_DATA_PATH = '/home/arthas/berkeley/data'
BDB_LOG_PATH = '/home/arthas/berkeley/log'
BDB_TMP_PATH = '/home/arthas/berkeley/tmp'


def load_items(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchBdbRealWeiboDataR(hurdles.BenchCase):
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

        weibo_hash_db_user = db.DB(self.db_env)
        weibo_hash_db_user.open('weibo_hash_user', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_user = weibo_hash_db_user

        weibo_hash_db_retweeted_status = db.DB(self.db_env)
        weibo_hash_db_retweeted_status.open('weibo_hash_retweeted_status', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_retweeted_status = weibo_hash_db_retweeted_status

        weibo_hash_db_text = db.DB(self.db_env)
        weibo_hash_db_text.open('weibo_hash_text', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_text = weibo_hash_db_text

        weibo_hash_db_timestamp = db.DB(self.db_env)
        weibo_hash_db_timestamp.open('weibo_hash_timestamp', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_timestamp = weibo_hash_db_timestamp

        weibo_hash_db_reposts_count = db.DB(self.db_env)
        weibo_hash_db_reposts_count.open('weibo_hash_reposts_count', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_reposts_count = weibo_hash_db_reposts_count

        weibo_hash_db_source = db.DB(self.db_env)
        weibo_hash_db_source.open('weibo_hash_source', None, db.DB_HASH, db.DB_CREATE)
        self.weibo_hash_db_source = weibo_hash_db_source

    def tearDown(self):
        self.weibo_hash_db.close()
        self.weibo_hash_db_user.close()
        self.weibo_hash_db_retweeted_status.close()
        self.weibo_hash_db_text.close()
        self.weibo_hash_db_timestamp.close()
        self.weibo_hash_db_reposts_count.close()
        self.weibo_hash_db_source.close()

        self.db_env.close()

    def _load_items(self, limit):
        weibo = []
        bs_input = load_items()
        count = 0
        for _, item in bs_input.reads():
            weibo.append(item)
            count += 1
            if count == limit:
                break

        return weibo

    def bench_get_whole(self):
        for weibo in self.weibos:
            msgpack.unpackb(self.weibo_hash_db.get(str(weibo['_id'])))

    def bench_get_sperate_text(self):
        for weibo in self.weibos:
            self.weibo_hash_db_text.get(str(weibo['_id']))

    def bench_get_sperate_text_timestamp(self):
        for weibo in self.weibos:
            self.weibo_hash_db_text.get(str(weibo['_id']))
            float(self.weibo_hash_db_timestamp.get(str(weibo['_id'])))

    def bench_get_sperate_timestamp(self):
        for weibo in self.weibos:
            float(self.weibo_hash_db_timestamp.get(str(weibo['_id'])))

    def bench_get_sperate_timestamp_user_reposts_count(self):
        for weibo in self.weibos:
            float(self.weibo_hash_db_timestamp.get(str(weibo['_id'])))
            int(self.weibo_hash_db_user.get(str(weibo['_id'])))
            int(self.weibo_hash_db_reposts_count.get(str(weibo['_id'])))

    """
    hurdles bench_bdb_real_weibo_data_r.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataR.bench_get_sperate_text
     | average       435.381 ms
     | median        433.07 ms
     | fastest       432.41 ms
     | slowest       453.75 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataR.bench_get_sperate_text_timestamp
     | average       913.766 ms
     | median        922.21 ms
     | fastest       872.88 ms
     | slowest       967.02 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataR.bench_get_sperate_timestamp
     | average       471.978 ms
     | median        471.165 ms
     | fastest       469.73 ms
     | slowest       480.26 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataR.bench_get_sperate_timestamp_user_reposts_count
     | average       1758.962 ms
     | median        1451.065 ms
     | fastest       1443.22 ms
     | slowest       4526.24 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataR.bench_get_whole
     | average       1344.407 ms
     | median        736.87 ms
     | fastest       734.8 ms
     | slowest       6711.59 ms

    综合来看还是整体一次取出来比较合适
    """

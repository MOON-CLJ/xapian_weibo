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

KEYS2STORE = ['user', 'retweeted_status', 'text', '_id', 'timestamp', 'reposts_count', 'source']  # bmiddle_pic,geo暂不处理
pre_func = {
    'user': lambda x: x['id'],
    'retweeted_status': lambda x: x['id'],
}

def load_items(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


class BenchBdbRealWeiboDataW(hurdles.BenchCase):
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
            item = dict([(k, pre_func[k](item[k] if k in item else {'id': 0}) if k in pre_func else item[k]) for k in KEYS2STORE])
            weibo.append(item)
            count += 1
            if count == limit:
                break

        return weibo

    def bench_write_whole(self):
        for weibo in self.weibos:
            self.weibo_hash_db.put(str(weibo['_id']), msgpack.packb(weibo))

    def bench_write_sperate(self):
        for weibo in self.weibos:
            self.weibo_hash_db_user.put(str(weibo['_id']), str(weibo['user']))
            self.weibo_hash_db_retweeted_status.put(str(weibo['_id']), str(weibo['retweeted_status']))
            self.weibo_hash_db_text.put(str(weibo['_id']), weibo['text'].encode('utf8'))
            self.weibo_hash_db_timestamp.put(str(weibo['_id']), str(weibo['timestamp']))
            self.weibo_hash_db_reposts_count.put(str(weibo['_id']), str(weibo['reposts_count']))
            self.weibo_hash_db_source.put(str(weibo['_id']), weibo['source'].encode('utf8'))

    """
    hurdles bench_bdb_real_weibo_data_w.py
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataW.bench_write_sperate
     | average       4848.811 ms
     | median        2879.535 ms
     | fastest       2874.17 ms
     | slowest       15705.7 ms
    bson file mode: 从备份的BSON文件中加载微博
    BenchBdbRealWeiboDataW.bench_write_whole
     | average       1150.215 ms
     | median        1144.04 ms
     | fastest       1141.92 ms
     | slowest       1208.83 ms
    """

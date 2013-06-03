# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from xapian_backend import Schema
import sys
import os
import signal
import msgpack
import bsddb3
from bsddb3 import db
import zmq
import time
import datetime


SCHEMA_VERSION = 2
PROCESS_IDX_SIZE = 100000
BDB_ORIGIN_DATA_PATH_BASE = '/home/arthas/berkeley/%s'


class OriginData2Bdb(object):
    def __init__(self, dbpath, schema_version):
        self.schema = getattr(Schema, 'v%s' % schema_version)
        db_path = BDB_ORIGIN_DATA_PATH_BASE % dbpath
        db_data_path = os.path.join(db_path, 'data')
        db_log_path = os.path.join(db_path, 'log')
        db_tmp_path = os.path.join(db_path, 'tmp')

        self.db_env = db.DBEnv()
        self.db_env.set_tmp_dir(db_tmp_path)
        self.db_env.set_lg_dir(db_log_path)
        self.db_env.set_cachesize(0, 8 * (2 << 27), 1)
        try:
            self.db_env.open(db_data_path, db.DB_INIT_CDB | db.DB_INIT_MPOOL | db.DB_CREATE)
        except db.DBNoSuchFileError:
            print 'maybe you should "mkdir -p %s/[data | tmp | log]"' % db_path
            raise

        hash_db = db.DB(self.db_env)
        hash_db.open('%s_hash' % dbpath, None, db.DB_HASH, db.DB_CREATE)
        self.hash_db = hash_db

    def update(self, item):
        if 'dumps_exclude' in self.schema:
            for k in self.schema['dumps_exclude']:
                if k in item:
                    del item[k]

        if 'pre' in self.schema:
            for k in self.schema['pre']:
                if k in item:
                    item[k] = self.schema['pre'][k](item[k])

        self.hash_db.put(str(item['_id']), msgpack.packb(item))

    def close(self):
        self.hash_db.close()
        self.db_env.close()

if __name__ == "__main__":
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    parser = ArgumentParser()
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')

    args = parser.parse_args(sys.argv[1:])
    dbpath = args.dbpath

    bdb_writer = OriginData2Bdb(dbpath, SCHEMA_VERSION)

    def signal_handler(signal, frame):
        bdb_writer.close()
        print 'worker stop, finally close db'
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    # Process index forever
    count = 0
    ts = time.time()
    while 1:
        item = receiver.recv_json()
        bdb_writer.update(item)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            cost = te - ts
            ts = te
            print '[%s] num indexed: %s %s sec/per %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, cost, PROCESS_IDX_SIZE)

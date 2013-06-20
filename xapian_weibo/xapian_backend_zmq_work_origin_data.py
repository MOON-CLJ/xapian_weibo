# -*- coding: utf-8 -*-
# 此脚本能将weibo数据项正确写入tokudb

from argparse import ArgumentParser
from xapian_backend import Schema
import _mysql_exceptions
import MySQLdb
import sys
import os
import signal
import msgpack
import zmq
import time
import datetime


SCHEMA_VERSION = 2
PROCESS_IDX_SIZE = 100000

iter_keys = ['_id', 'user', 'retweeted_status', 'text', 'timestamp', 'reposts_count', 'source', 'bmiddle_pic', 'geo']
pre_func = {
    'user': lambda x: x['id'] if x else 0,
    'retweeted_status': lambda x: x['id'] if x else 0,
    'geo': lambda x: msgpack.packb(x) if x else None,
}

class OriginData2Tokudb(object):
    def __init__(self, dbpath, schema_version):
        self.schema = getattr(Schema, 'v%s' % schema_version)
        conn = MySQLdb.connect(user='root', passwd='', db='master_timeline')
        self.conn = conn
        self.cursor = conn.cursor()

    def update(self, items):
        batch_data = []
        for item in items:
            item = dict([(k, pre_func[k](item.get(k)) if k in pre_func else item.get(k)) for k in iter_keys])
            for k, v in item.iteritems():
                if k in ['text', 'source', 'bmiddle_pic'] and isinstance(v, unicode):
                    item[k] = v.encode('utf8')

            batch_data.append([item[i] for i in iter_keys])

        self.cursor.executemany(
            'insert into master_timeline_weibo (' + ', '.join(iter_keys) + ') values (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
            batch_data
        )
        """
        except _mysql_exceptions.IntegrityError:
            pass
        """
        self.conn.commit()

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    parser = ArgumentParser()
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')

    args = parser.parse_args(sys.argv[1:])
    dbpath = args.dbpath

    tokudb_writer = OriginData2Tokudb(dbpath, SCHEMA_VERSION)

    # Process index forever
    count = 0
    batch_data = []
    batch_size = 10000

    def signal_handler(signal, frame):
        tokudb_writer.update(batch_data)
        print 'write last batch:', len(batch_data)
        tokudb_writer.close()
        print 'worker stop, finally close db'
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    ts = time.time()
    while 1:
        item = receiver.recv_json()
        batch_data.append(item)
        count += 1
        if count % batch_size == 0:
            tokudb_writer.update(batch_data)
            batch_data = []

        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            cost = te - ts
            ts = te
            print '[%s] num indexed: %s %s sec/per %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, cost, PROCESS_IDX_SIZE)

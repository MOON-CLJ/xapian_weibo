# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from xapian_backend import _database, Schema, DOCUMENT_ID_TERM_PREFIX, \
    InvalidIndexError, _index_field
from utils import load_scws
import sys
import os
import signal
import xapian
import msgpack
import zmq
import time
import datetime


SCHEMA_VERSION = 2
PROCESS_IDX_SIZE = 10000


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, pid):
        self.schema = getattr(Schema, 'v%s' % schema_version)
        self.db_folder = "_%s_%s" % (dbpath, pid)
        self.s = load_scws()
        self.db = _database(self.db_folder, writable=True)

    def document_count(self):
        try:
            return _database(self.db_folder).get_doccount()
        except InvalidIndexError:
            return 0

    def update(self, item):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + str(item[self.schema['obj_id']])
        for field in self.schema['idx_fields']:
            self.index_field(field, document, item, SCHEMA_VERSION)
        if 'dumps_exclude' in self.schema:
            for k in self.schema['dumps_exclude']:
                if k in item:
                    del item[k]

        if 'pre' in self.schema:
            for k in self.schema['pre']:
                if k in item:
                    item[k] = self.schema['pre'][k](item[k])

        document.add_term(document_id)
        #self.db.replace_document(document_id, document)
        self.db.add_document(document)

    def index_field(self, field, document, item, schema_version):
        _index_field(field, document, item, schema_version, self.schema)

    def close(self):
        self.db.close()
        print 'total index', self.document_count()


if __name__ == "__main__":
    """
    cd data/
    then run 'py ../xapian_weibo/xapian_backend_zmq_work.py hehe'
    """
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5557")

    parser = ArgumentParser()
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')

    args = parser.parse_args(sys.argv[1:])
    dbpath = args.dbpath

    pid = os.getpid()
    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION, pid=pid)

    def signal_handler(signal, frame):
        xapian_indexer.close()
        print 'worker stop, finally close db'
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    # Process index forever
    count = 0
    ts = time.time()
    while 1:
        item = receiver.recv_json()
        xapian_indexer.update(item)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            cost = te - ts
            ts = te
            print '[%s] folder[%s] num indexed: %s %s sec/per %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), xapian_indexer.db_folder, count, cost, PROCESS_IDX_SIZE)

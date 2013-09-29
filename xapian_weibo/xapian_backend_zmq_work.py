# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from consts import SCHEMA_VERSION, XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_VENT_PORT, \
        XAPIAN_STUB_FILE_DIR, XAPIAN_DATA_DIR, XAPIAN_FLUSH_DB_SIZE
from xapian_backend import _database, Schema, DOCUMENT_ID_TERM_PREFIX, \
    InvalidIndexError, _index_field
from utils import load_scws, log_to_stub
import sys
import os
import signal
import xapian
import msgpack
import zmq
import time
import datetime


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, pid):
        self.dbpath = dbpath
        self.schema = getattr(Schema, 'v%s' % schema_version)
        self.db_folder = os.path.join(XAPIAN_DATA_DIR, '_%s_%s' % (dbpath, pid))
        self.s = load_scws()
        self.db = _database(self.db_folder, writable=True)

        self.termgen = xapian.TermGenerator()
        self.iter_keys = self.schema['origin_data_iter_keys']
        self.pre_func = self.schema.get('pre_func', {})

    def document_count(self):
        try:
            return _database(self.db_folder).get_doccount()
        except InvalidIndexError:
            return 0

    def add(self, item):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + str(item[self.schema['obj_id']])
        for field in self.schema['idx_fields']:
            self.index_field(field, document, item, SCHEMA_VERSION)

        # origin_data跟term和value的处理方式不一样
        item = dict([(k, self.pre_func[k](item.get(k)) if k in self.pre_func and item.get(k) else item.get(k))
                     for k in self.iter_keys])
        document.set_data(msgpack.packb(item))
        document.add_term(document_id)
        # self.db.replace_document(document_id, document)
        self.db.add_document(document)

    def index_field(self, field, document, item, schema_version):
        _index_field(field, document, item, schema_version, self.schema, self.termgen)

    def _log_to_stub(self):
        log_to_stub(XAPIAN_STUB_FILE_DIR, self.dbpath, self.db_folder)

    def close(self):
        self.db.close()
        self._log_to_stub()
        print 'total index', self.document_count()


if __name__ == '__main__':
    """
    cd data/
    py ../xapian_weibo/xapian_backend_zmq_work.py hehe
    """
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_VENT_PORT))

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
    signal.signal(signal.SIGTERM, signal_handler)

    # Process index forever
    count = 0
    ts = time.time()
    while 1:
        item = receiver.recv_json()
        xapian_indexer.add(item)
        count += 1
        if count % XAPIAN_FLUSH_DB_SIZE == 0:
            te = time.time()
            cost = te - ts
            ts = te
            print '[%s] folder[%s] num indexed: %s %s sec/per %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), xapian_indexer.db_folder, count, cost, XAPIAN_FLUSH_DB_SIZE)

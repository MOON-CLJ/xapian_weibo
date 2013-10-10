# -*- coding: utf-8 -*-

from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_ZMQ_VENT_HOST, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT, \
    XAPIAN_STUB_FILE_DIR, XAPIAN_DATA_DIR, XAPIAN_FLUSH_DB_SIZE, \
    XAPIAN_DB_PATH, XAPIAN_ZMQ_WORK_KILL_INTERVAL
from xapian_backend import _database, Schema, DOCUMENT_ID_TERM_PREFIX, \
    InvalidIndexError, _index_field
from utils import load_scws, log_to_stub

from argparse import ArgumentParser
from datetime import datetime
import sys
import os
import signal
import xapian
import msgpack
import zmq
import time

SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, pid, remote_stub):
        self.dbpath = dbpath
        self.remote_stub = remote_stub
        self.schema = getattr(Schema, 'v%s' % schema_version)
        today_date_str = datetime.now().date().strftime("%Y%m%d")
        self.db_folder = os.path.join(XAPIAN_DATA_DIR, '%s/_%s_%s' % (today_date_str, dbpath, pid))
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
        log_to_stub(XAPIAN_STUB_FILE_DIR, self.dbpath, self.db_folder, remote_stub=self.remote_stub)

    def close(self):
        self.db.close()
        self._log_to_stub()
        print 'total index', self.document_count()


if __name__ == '__main__':
    """
    cd data/
    py ../xapian_weibo/xapian_backend_zmq_work.py -r
    """
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_VENT_PORT))

    # Socket for control input
    controller = context.socket(zmq.SUB)
    controller.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_CTRL_VENT_PORT))
    controller.setsockopt(zmq.SUBSCRIBE, "")

    # Process messages from receiver and controller
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(controller, zmq.POLLIN)

    parser = ArgumentParser()
    parser.add_argument('-r', '--remote_stub', action='store_true', help='remote stub')
    args = parser.parse_args(sys.argv[1:])
    remote_stub = args.remote_stub

    dbpath = XAPIAN_DB_PATH
    pid = os.getpid()
    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION, pid, remote_stub)

    # Process index forever
    count = 0
    ts = time.time()
    tb = ts
    receive_kill = False
    while 1:
        socks = dict(poller.poll())
        if socks.get(receiver) == zmq.POLLIN:
            item = receiver.recv_json()
            xapian_indexer.add(item)
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                cost = te - ts
                ts = te
                print '[%s] [%s] total indexed: %s, %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), xapian_indexer.db_folder, count, cost, XAPIAN_FLUSH_DB_SIZE)
        elif receive_kill and time.time() - tb > XAPIAN_ZMQ_WORK_KILL_INTERVAL:
            """
            定期kill，可以记录work开启的时间
            然后收到kill的时候判断一下当前时间减去work开启的时间
            是否超过某个阈值，是则执行kill操作
            配套的prod模式下，应该在每隔XAPIAN_ZMQ_WORK_KILL_INTERVAL新开work
            """
            xapian_indexer.close()
            print 'receive "KILL", worker stop, finally close db, cost: %ss' % (time.time() - tb)
            break

        # Any waiting controller command acts as 'KILL'
        if socks.get(controller) == zmq.POLLIN:
            receive_kill = True

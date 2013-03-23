# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from xapian_backend import _marshal_value, _marshal_term, _database, Schema, \
    DOCUMENT_ID_TERM_PREFIX, DOCUMENT_CUSTOM_TERM_PREFIX, single_word_whitelist, \
    InvalidIndexError
from utils import load_scws
import sys
import os
import signal
import xapian
import simplejson as json
import zmq
import time
import datetime


SCHEMA_VERSION = 1
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

    def update(self, weibo):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + str(weibo[self.schema['obj_id']])
        for field in self.schema['idx_fields']:
            self.index_field(field, document, weibo, SCHEMA_VERSION)
        if 'dumps_exclude' in self.schema:
            for k in self.schema['dumps_exclude']:
                if k in weibo:
                    del weibo[k]

        document.set_data(json.dumps(weibo))
        document.add_term(document_id)
        self.db.replace_document(document_id, document)

    def index_field(self, field, document, weibo, schema_version):
        prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field['field_name'].upper()
        if schema_version == 1:
            if field['field_name'] == 'uid':
                term = _marshal_term(weibo[field['field_name']])
                document.add_term(prefix + term)
            elif field['field_name'] == 'ts':
                document.add_value(field['column'], _marshal_value(weibo[field['field_name']]))
            elif field['field_name'] == 'text':
                tokens = [token[0] for token
                          in self.s.participle(weibo[field['field_name']].encode('utf-8'))
                          if 3 < len(token[0]) < 10 or token[0] in single_word_whitelist]
                termgen = xapian.TermGenerator()
                termgen.set_document(document)
                termgen.index_text_without_positions(' '.join(tokens), 1, prefix)
        elif schema_version == 2:
            if field['field_name'] in ['user', 'retweeted_status']:
                if 'retweeted_status' not in weibo:
                    return
                term = _marshal_term(weibo[field['field_name']], self.schema['pre'][field['field_name']])
                document.add_term(prefix + term)
            elif field['field_name'] in ['timestamp', 'reposts_count', 'comments_count', 'attitudes_count']:
                document.add_value(field['column'], _marshal_value(weibo[field['field_name']]))
            elif field['field_name'] == 'text':
                tokens = [token[0] for token
                          in self.s.participle(weibo[field['field_name']].encode('utf-8'))
                          if 3 < len(token[0]) < 10 or token[0] in single_word_whitelist]
                termgen = xapian.TermGenerator()
                termgen.set_document(document)
                termgen.index_text_without_positions(' '.join(tokens), 1, prefix)

    def close(self):
        self.db.close()


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
        weibo = receiver.recv_json()
        xapian_indexer.update(weibo)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            cost = te - ts
            ts = te
            print '[%s] folder[%s] num indexed: %s %s sec/per %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), xapian_indexer.db_folder, count, cost, PROCESS_IDX_SIZE)

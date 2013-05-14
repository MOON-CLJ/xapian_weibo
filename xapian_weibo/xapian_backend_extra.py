#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from query_base import Q, notQ
from xapian_backend import timeit, _marshal_value, _marshal_term, _database, InvalidIndexError, OperationError
from xapian_backend import XapianSearch as XapianSearchWeibo
import os
import sys
import xapian
import leveldb
import msgpack
import datetime
import calendar
import time


PROCESS_IDX_SIZE = 20000
SCHEMA_VERSION = 1
DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'

LEVELDBPATH = '/home/mirage/leveldb'
weibo_multi_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_multi_sentiment'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, refresh_db=False):
        self.path = dbpath
        self.schema = getattr(Schema, 'v%s' % schema_version)
        self.refresh_db = refresh_db

        self.databases = {}
        self.ts_and_dbfolders = []

    def document_count(self, folder):
        try:
            return self.get_database(folder, writable=False).get_doccount()
        except InvalidIndexError:
            return 0

    def generate(self, start_time=None):
        if start_time:
            start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
            folder = "_%s_%s" % (self.path, start_time.strftime('%Y-%m-%d'))
            self.ts_and_dbfolders.append((calendar.timegm(start_time.timetuple()), folder))
        else:
            start_time = datetime.datetime(2009, 8, 1)
            step_time = datetime.timedelta(days=50)
            while start_time < datetime.datetime.today():
                folder = "_%s_%s" % (self.path, start_time.strftime('%Y-%m-%d'))
                self.ts_and_dbfolders.append((calendar.timegm(start_time.timetuple()), folder))
                start_time += step_time

    def get_database(self, folder, writable=True):
        if folder not in self.databases:
            self.databases[folder] = _database(folder, writable=writable, refresh=self.refresh_db)
        return self.databases[folder]

    @timeit
    def index_items(self):
        count = 0
        try:
            for item in _load_weibos_from_xapian(fields=['_id', 'user', 'terms', 'timestamp']):
                count += 1
                if 'posted_at_key' not in self.schema:
                    raise Exception('当前mode下需要schema里包含区分folder的posted_at_key')
                posted_at = item[self.schema['posted_at_key']]
                for i in xrange(len(self.ts_and_dbfolders) - 1):
                    if self.ts_and_dbfolders[i][0] <= posted_at < self.ts_and_dbfolders[i + 1][0]:
                        folder = self.ts_and_dbfolders[i][1]
                        break
                else:
                    if posted_at >= self.ts_and_dbfolders[i + 1][0]:
                        folder = self.ts_and_dbfolders[i + 1][1]

                self.update(folder, item)
                if count % PROCESS_IDX_SIZE == 0:
                    print '[%s] folder[%s] num indexed: %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), folder, count)
        except Exception:
            raise

        finally:
            for database in self.databases.itervalues():
                database.close()

            for _, folder in self.ts_and_dbfolders:
                print '[', folder, ']', 'total size', self.document_count(folder)

    def update(self, folder, item):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + str(item[self.schema['obj_id']])
        for field in self.schema['idx_fields']:
            self.index_field(field, document, item, SCHEMA_VERSION)

        document.set_data(msgpack.packb({}))
        document.add_term(document_id)
        self.get_database(folder).replace_document(document_id, document)
        #self.get_database(folder).add_document(document)

    def index_field(self, field, document, item, schema_version):
        _index_field(field, document, item, schema_version, self.schema)


def _index_field(field, document, item, schema_version, schema):
    prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field['field_name'].upper()
    if schema_version == 1:
        # 必选term
        if field['field_name'] in ['user']:
            term = _marshal_term(item[field['field_name']])
            document.add_term(prefix + term)
        elif field['field_name'] == 'sentiment':
            sentiment = weibo_multi_sentiment_bucket.get(str(item[self.schema['obj_id']]))
            sentiment = int(sentiment)
            term = _marshal_term(sentiment)
            document.add_term(prefix + term)
        # value
        elif field['field_name'] in ['_id', 'timestamp']:
            document.add_value(field['column'], _marshal_value(item[field['field_name']]))
        elif field['field_name'] == 'text':
            tokens = item['terms']
            termgen = xapian.TermGenerator()
            termgen.set_document(document)
            termgen.index_text_without_positions(' '.join(tokens), 1, prefix)


@timeit
def _load_weibos_from_xapian(total_days=90, fields=['_id', 'retweeted_status', 'text']):
    total_days = 90
    today = datetime.datetime.today()
    end_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    begin_ts = end_ts - total_days * 24 * 3600

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }
    s = XapianSearchWeibo(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')

    count, get_results = s.search(query=query_dict, fields=fields)
    print count
    return get_results


class Schema:
    v1 = {
        'obj_id': '_id',
        # 用于去重的value no(column)
        'collapse_valueno': 3,
        'posted_at_key': 'timestamp',
        'idx_fields': [
            # term
            {'field_name': 'user', 'column': 0, 'type': 'long'},
            {'field_name': 'text', 'column': 1, 'type': 'text'},
            # extra term
            {'field_name': 'sentiment', 'column': 2, 'type': 'int'},
            # value
            {'field_name': '_id', 'column': 3, 'type': 'long'},
            {'field_name': 'timestamp', 'column': 4, 'type': 'long'},
        ],
    }


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')

    args = parser.parse_args(sys.argv[1:])
    dbpath = args.dbpath

    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION, refresh_db=debug)
    xapian_indexer.generate()
    xapian_indexer.index_weibos()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 实测 gevent在此起不了任何优化作用

import sys
import xapian
import cPickle as pickle
import simplejson as json
import pymongo
import scws
import datetime
import gevent
from gevent.queue import Queue, Empty


SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
CUSTOM_DICT_PATH = '../dict/userdic.txt'
IGNORE_PUNCTUATION = 1
EXTRA_STOPWORD_PATH = '../dict/stopword.dic'
EXTRA_EMOTIONWORD_PATH = '../dict/emotionlist.txt'
PROCESS_IDX_SIZE = 1000000

SCHEMA_VERSION = 1
DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'


class XapianBackend(object):
    def __init__(self, dbpath, schema_version):
        self.path = dbpath
        if schema_version == 1:
            self.schema = Schema.v1

        self.queues = {}
        self.generate()
        self.load_scws()
        self.load_mongod()
        self.load_extra_dic()

    def document_count(self, folder):
        try:
            return _database(folder).get_doccount()
        except InvalidIndexError:
            return 0

    def generate(self):
        folders_with_date = []
        start_time = datetime.datetime(2009, 8, 1)
        step_time = datetime.timedelta(days=50)
        while start_time < datetime.datetime.today():
            folder = "_%s_%s" % (self.path, start_time.strftime("%Y-%m-%d"))
            folders_with_date.append((start_time, folder))
            self.queues[folder] = Queue()
            start_time += step_time

        self.folders_with_date = folders_with_date

    def load_extra_dic(self):
        self.emotion_words = [line.strip('\r\n') for line in file(EXTRA_EMOTIONWORD_PATH)]

    def load_scws(self):
        s = scws.Scws()
        s.set_charset(SCWS_ENCODING)

        s.set_dict(CHS_DICT_PATH, scws.XDICT_MEM)
        s.add_dict(CHT_DICT_PATH, scws.XDICT_MEM)
        s.add_dict(CUSTOM_DICT_PATH, scws.XDICT_TXT)

        # 把停用词全部拆成单字，再过滤掉单字，以达到去除停用词的目的
        s.add_dict(EXTRA_STOPWORD_PATH, scws.XDICT_TXT)
        # 即基于表情表对表情进行分词，必要的时候在返回结果处或后剔除
        s.add_dict(EXTRA_EMOTIONWORD_PATH, scws.XDICT_TXT)

        s.set_rules(SCWS_RULES)
        s.set_ignore(IGNORE_PUNCTUATION)
        self.s = s

    def load_mongod(self):
        connection = pymongo.Connection()
        db = connection.admin
        db.authenticate('root', 'root')
        db = connection.weibo
        self.db = db

    #@profile
    def load_and_index_weibos(self):
        count = 0
        if debug:
            with open("../test/sample_tweets.js") as f:
                weibos = json.loads(f.readline())
            print 'loaded weibos from file'
        else:
            weibos = self.db.statuses.find()

        for weibo in weibos:
            count += 1
            posted_at = datetime.datetime.fromtimestamp(weibo[self.schema['posted_at_key']])
            for i in xrange(len(self.folders_with_date) - 1):
                if self.folders_with_date[i][0] <= posted_at < self.folders_with_date[i + 1][0]:
                    folder = self.folders_with_date[i][1]
                    break
            else:
                if posted_at >= self.folders_with_date[i + 1][0]:
                    folder = self.folders_with_date[i + 1][1]
            self.queues[folder].put_nowait(weibo)

            if count % PROCESS_IDX_SIZE == 0:
                self.update()

        if count % PROCESS_IDX_SIZE != 0:
            self.update()

        if debug:
            for _, folder in self.folders_with_date:
                print 'index size', folder, self.document_count(folder)

    def index_field(self, field, document, weibo, schema_version):
        prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field['field_name'].upper()
        if schema_version == 1:
            if field['field_name'] in ['uid', 'name']:
                term = _marshal_term(weibo[field['field_name']])
                document.add_term(prefix + term)
                document.add_value(field['column'], weibo[field['field_name']])
            elif field['field_name'] == 'ts':
                document.add_value(field['column'], _marshal_value(weibo[field['field_name']]))
            elif field['field_name'] == 'text':
                tokens = [token[0] for token
                          in self.s.participle(weibo[field['field_name']].encode('utf-8'))
                          if len(token[0]) > 1]
                for token in tokens:
                    document.add_term(prefix + token)

                document.add_value(field['column'], weibo[field['field_name']])

    def update_worker(self, folder, queue):
        if debug:
            print 'index begin', folder, queue.qsize()
        database = _database(folder, writable=True)
        try:
            while 1:
                weibo = queue.get(timeout=1)

                document = xapian.Document()
                document_id = DOCUMENT_ID_TERM_PREFIX + weibo[self.schema['obj_id']]
                for field in self.schema['idx_fields']:
                    self.index_field(field, document, weibo, SCHEMA_VERSION)

                document.set_data(pickle.dumps(
                    weibo, pickle.HIGHEST_PROTOCOL
                ))
                document.add_term(document_id)
                database.replace_document(document_id, document)

        except Empty:
            if debug:
                print 'index done', folder
        except:
            sys.stderr.write('Chunk failed.\n')
            raise

        finally:
            database.close()

    def update(self):
        if debug:
            print "** " * 10
        gevent.joinall([
            gevent.spawn(self.update_worker, k, v)
            for k, v in self.queues.iteritems() if v.qsize()
        ])

    def search(self):
        pass


class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass


class Schema:
    v1 = {
        'obj_id': '_id',
        'posted_at_key': 'ts',
        'idx_fields': [
            {'field_name': 'uid', 'column': 0},
            {'field_name': 'name', 'column': 1},
            {'field_name': 'text', 'column': 2},
            {'field_name': 'ts', 'column': 3}
        ],
    }


def _database(folder, writable=False):
    """
    Private method that returns a xapian.Database for use.

    Optional arguments:
        ``writable`` -- Open the database in read/write mode (default=False)

    Returns an instance of a xapian.Database or xapian.WritableDatabase
    """
    if writable:
        if debug:
            database = xapian.WritableDatabase(folder, xapian.DB_CREATE_OR_OVERWRITE)
        else:
            database = xapian.WritableDatabase(folder, xapian.DB_CREATE_OR_OPEN)
    else:
        try:
            database = xapian.Database(folder)
        except xapian.DatabaseOpeningError:
            raise InvalidIndexError(u'Unable to open index at %s' % folder)

    return database


def _marshal_value(value):
    """
    Private utility method that converts Python values to a string for Xapian values.
    """
    if isinstance(value, (int, long)):
        value = xapian.sortable_serialise(value)
    return value


def _marshal_term(term):
    """
    Private utility method that converts Python terms to a string for Xapian terms.
    """
    if isinstance(term, int):
        term = str(term)
    return term


if __name__ == "__main__":
    debug = True  # debug True run 'py -m memory_profiler ../xapian_weibo/xapian_backend.py hehe'
    if debug:
        PROCESS_IDX_SIZE = 100000
    if not debug and len(sys.argv) != 2:
        print >> sys.stderr, "Usage: %s PATH_TO_DATABASE" % sys.argv[0]
        sys.exit(1)

    if debug:
        dbpath = sys.argv[2]
    else:
        dbpath = sys.argv[1]
    xapian_backend = XapianBackend(dbpath, SCHEMA_VERSION)
    xapian_backend.load_and_index_weibos()

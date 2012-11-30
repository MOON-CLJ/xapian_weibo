#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import xapian
import cPickle as pickle
import simplejson as json
import pymongo
import scws
import datetime
import calendar
from argparse import ArgumentParser


SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
CUSTOM_DICT_PATH = '../dict/userdic.txt'
IGNORE_PUNCTUATION = 1
EXTRA_STOPWORD_PATH = '../dict/stopword.dic'
EXTRA_EMOTIONWORD_PATH = '../dict/emotionlist.txt'
PROCESS_IDX_SIZE = 100000

SCHEMA_VERSION = 1
DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'


class XapianIndex(object):
    def __init__(self, dbpath, schema_version):
        self.path = dbpath
        self.schema = getattr(Schema, 'v%s' % schema_version, None)

        self.databases = {}
        self.load_scws()
        self.load_mongod()
        self.load_extra_dic()

    def document_count(self, folder):
        try:
            return _database(folder).get_doccount()
        except InvalidIndexError:
            return 0

    def generate(self, start_time=None):
        folders_with_date = []

        if not debug and start_time:
            start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
            folder = "_%s_%s" % (self.path, start_time.strftime('%Y-%m-%d'))
            folders_with_date.append((start_time, folder))
        elif debug:
            start_time = datetime.datetime(2009, 8, 1)
            step_time = datetime.timedelta(days=50)
            while start_time < datetime.datetime.today():
                folder = "_%s_%s" % (self.path, start_time.strftime('%Y-%m-%d'))
                folders_with_date.append((start_time, folder))
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

    def get_database(self, folder):
        if folder not in self.databases:
            self.databases[folder] = _database(folder, writable=True)
        return self.databases[folder]

    #@profile
    def load_and_index_weibos(self, start_time=None):
        if not debug and start_time:
            start_time = self.folders_with_date[0][0]
            end_time = start_time + datetime.timedelta(days=50)
            weibos = self.db.statuses.find({
                self.schema['posted_at_key']: {
                    '$gte': calendar.timegm(start_time.timetuple()),
                    '$lt': calendar.timegm(end_time.timetuple())
                }
            }, timeout=False)
            print 'prod mode: loaded weibos from mongod'
        elif debug:
            with open("../test/sample_tweets.js") as f:
                weibos = json.loads(f.readline())
            print 'debug mode: loaded weibos from file'

        count = 0
        try:
            for weibo in weibos:
                count += 1
                posted_at = datetime.datetime.fromtimestamp(weibo[self.schema['posted_at_key']])
                if not debug and start_time:
                    folder = self.folders_with_date[0][1]
                elif debug:
                    for i in xrange(len(self.folders_with_date) - 1):
                        if self.folders_with_date[i][0] <= posted_at < self.folders_with_date[i + 1][0]:
                            folder = self.folders_with_date[i][1]
                            break
                    else:
                        if posted_at >= self.folders_with_date[i + 1][0]:
                            folder = self.folders_with_date[i + 1][1]

                self.update(folder, weibo)
                if count % PROCESS_IDX_SIZE == 0:
                    print '[%s] folder[%s] num indexed: %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), folder, count)
        except Exception:
            raise

        finally:
            for database in self.databases.itervalues():
                database.close()

            for _, folder in self.folders_with_date:
                print 'index size', folder, self.document_count(folder)

    def update(self, folder, weibo):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + weibo[self.schema['obj_id']]
        for field in self.schema['idx_fields']:
            self.index_field(field, document, weibo, SCHEMA_VERSION)

        document.set_data(pickle.dumps(
            weibo, pickle.HIGHEST_PROTOCOL
        ))
        document.add_term(document_id)
        self.get_database(folder).replace_document(document_id, document)

    def index_field(self, field, document, weibo, schema_version):
        prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field['field_name'].upper()
        if schema_version == 1:
            if field['field_name'] in ['uid', 'name']:
                term = _marshal_term(weibo[field['field_name']])
                document.add_term(prefix + term)
            elif field['field_name'] == 'ts':
                document.add_value(field['column'], _marshal_value(weibo[field['field_name']]))
            elif field['field_name'] == 'text':
                tokens = [token[0] for token
                          in self.s.participle(weibo[field['field_name']].encode('utf-8'))
                          if len(token[0]) > 1]
                for token in tokens:
                    if len(token) <= 10:
                        document.add_term(prefix + token)

                document.add_value(field['column'], weibo[field['field_name']])


class XapianSearch(object):
    def __init__(self, path='../data/', name='statuses', schema_version=SCHEMA_VERSION):
        def create(dbpath):
            return xapian.Database(dbpath)

        def merge(db1, db2):
            db1.add_database(db2)
            return db1

        self.database = reduce(merge,
                               map(create,
                                   [path + p for p in os.listdir(path) if p.startswith('_%s' % name)]))

        self.schema = getattr(Schema, 'v%s' % schema_version, None)

    def parse_query(self, query_dict):
        """
        Given a `query_dict`, will attempt to return a xapian.Query

        Required arguments:
            ``query_dict`` -- A MongoDB style query dict to parse

        Returns a xapian.Query
        """
        if query_dict is None:
            return xapian.Query('')  # Match everything
        elif query_dict == {}:
            return xapian.Query()  # Match nothing

        qp = xapian.QueryParser()
        qp.set_database(self.database)

        query = xapian.Query('')
        field_prefix = {}
        field_type = {}
        field_col = {}
        for field_dict in self.schema['idx_fields']:
            fname = field_dict['field_name']
            field_col[fname] = field_dict['column']
            field_type[fname] = field_dict['type']
            field_prefix[fname] = DOCUMENT_CUSTOM_TERM_PREFIX + fname.upper()

        for field in query_dict:
            if field in field_prefix:
                prefix = field_prefix[field]
                col = field_col[field]
                value = query_dict[field]

                if isinstance(value, dict):
                    ftype = field_type[field]
                    if ftype == 'int' or ftype == 'long':
                        begin = value.get('$gt', 0)
                        end = value.get('$lt', sys.maxint)
                        qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(col, '%s' % prefix))
                        new_query = qp.parse_query('%s%s..%s' % (prefix, begin, end))
                elif not hasattr(value, 'strip') and hasattr(value, '__getitem__') or hasattr(value, '__iter__'):
                    value = ['%s%s' % (prefix, v) for v in value]
                    new_query = xapian.Query(xapian.Query.OP_AND, value)
                else:
                    new_query = xapian.Query('%s%s' % (prefix, value))

                query = xapian.Query(xapian.Query.OP_AND, [query, new_query])

        return query

    def search(self, query=None, sort_by=None, start_offset=0,
               end_offset=1000, fields=None, **kwargs):

        query = self.parse_query(query)

        if xapian.Query.empty(query):
            return {
                'results': [],
                'hits': 0,
            }

        database = self.database
        enquire = xapian.Enquire(database)
        enquire.set_query(query)

        if sort_by:
            sorter = xapian.MultiValueSorter()

            for sort_field in sort_by:
                if sort_field.startswith('-'):
                    reverse = True
                    sort_field = sort_field[1:]  # Strip the '-'
                else:
                    reverse = False  # Reverse is inverted in Xapian -- http://trac.xapian.org/ticket/311
                sorter.add(self._value_column(sort_field), reverse)

            enquire.set_sort_by_key_then_relevance(sorter, True)

        results = []

        if not end_offset:
            end_offset = database.get_doccount() - start_offset

        matches = self._get_enquire_mset(database, enquire, start_offset, end_offset)

        for match in matches:
            weibo = pickle.loads(self._get_document_data(database, match.document))
            item = None
            if fields:
                item = {}
                for field in fields:
                    item[field] = weibo[field]
            else:
                item = weibo
            results.append(item)

        return {
            'results': results,
            'hits': self._get_hit_count(database, enquire)
        }

    def _get_enquire_mset(self, database, enquire, start_offset, end_offset):
        """
        A safer version of Xapian.enquire.get_mset

        Simply wraps the Xapian version and catches any `Xapian.DatabaseModifiedError`,
        attempting a `database.reopen` as needed.

        Required arguments:
            `database` -- The database to be read
            `enquire` -- An instance of an Xapian.enquire object
            `start_offset` -- The start offset to pass to `enquire.get_mset`
            `end_offset` -- The end offset to pass to `enquire.get_mset`
        """
        try:
            return enquire.get_mset(start_offset, end_offset)
        except xapian.DatabaseModifiedError:
            database.reopen()
            return enquire.get_mset(start_offset, end_offset)

    def _get_document_data(self, database, document):
        """
        A safer version of Xapian.document.get_data

        Simply wraps the Xapian version and catches any `Xapian.DatabaseModifiedError`,
        attempting a `database.reopen` as needed.

        Required arguments:
            `database` -- The database to be read
            `document` -- An instance of an Xapian.document object
        """
        try:
            return document.get_data()
        except xapian.DatabaseModifiedError:
            database.reopen()
            return document.get_data()

    def _value_column(self, field):
        """
        Private method that returns the column value slot in the database
        for a given field.

        Required arguemnts:
            `field` -- The field to lookup

        Returns an integer with the column location (0 indexed).
        """
        for field_dict in self.schema['idx_fields']:
            if field_dict['field_name'] == field:
                return field_dict['column']
        return 0

    def _get_hit_count(self, database, enquire):
        """
        Given a database and enquire instance, returns the estimated number
        of matches.

        Required arguments:
            `database` -- The database to be queried
            `enquire` -- The enquire instance
        """
        return self._get_enquire_mset(
            database, enquire, 0, database.get_doccount()
        ).size()


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


class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass


class Schema:
    v1 = {
        'obj_id': '_id',
        'posted_at_key': 'ts',
        'idx_fields': [
            {'field_name': 'uid', 'column': 0, 'type': 'long'},
            {'field_name': 'name', 'column': 1, 'type': 'text'},
            {'field_name': 'text', 'column': 2, 'type': 'text'},
            {'field_name': 'ts', 'column': 3, 'type': 'long'}
        ],
    }


if __name__ == "__main__":
    """
    cd to test/ folder
    then run 'py (-m memory_profiler) ../xapian_weibo/xapian_backend.py -d hehe'
    http://pypi.python.org/pypi/memory_profiler
    """
    parser = ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='DEBUG')
    parser.add_argument('-p', '--print_folders', action='store_true', help='PRINT FOLDER THEN EXIT')
    parser.add_argument('-s', '--start_time', nargs=1, help='DATETIME')
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')
    args = parser.parse_args(sys.argv[1:])
    debug = args.debug
    dbpath = args.dbpath

    if args.print_folders:
        debug = True
        xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION)
        xapian_indexer.generate()
        for _, folder in xapian_indexer.folders_with_date:
            print folder

        sys.exit(0)

    start_time = args.start_time[0] if args.start_time else None
    if debug:
        if start_time:
            print 'debug mode(warning): start_time will not be used'
        PROCESS_IDX_SIZE = 10000

    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION)
    xapian_indexer.generate(start_time)
    xapian_indexer.load_and_index_weibos(start_time)

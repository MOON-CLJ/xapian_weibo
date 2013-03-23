#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from query_base import Q, notQ
from utils import load_scws, load_one_words
from utils4scrapy.tk_maintain import _default_mongo
import os
import sys
import xapian
import simplejson as json
import datetime
import calendar
import time


PROCESS_IDX_SIZE = 20000
SCHEMA_VERSION = 1
DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017

single_word_whitelist = set(load_one_words())
single_word_whitelist |= set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, refresh_db=False):
        self.path = dbpath
        self.schema = getattr(Schema, 'v%s' % schema_version, None)
        self.refresh_db = refresh_db

        self.databases = {}
        self.ts_and_dbfolders = []
        self.s = load_scws()

        self.mgdb = _default_mongo(MONGOD_HOST, MONGOD_PORT, usedb=self.schema['db'])
        self.collection = self.schema['collection']

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

    #@profile

    def load_weibos(self, start_time=None, mode='debug'):
        if mode == 'idx_all':
            weibos = getattr(self.mgdb, self.collection).find(timeout=False)
            print 'prod mode: 从mongodb加载[%s:%s]里的所有微博' % (self.mgdb, self.collection)
        elif mode == 'single_given_db':
            if not start_time:
                raise Exception('single_given_db mode 需要指定start_time')
            start_time = self.ts_and_dbfolders[0][0]
            end_time = start_time + datetime.timedelta(days=50).total_seconds()
            weibos = getattr(self.mgdb, self.collection).find({
                self.schema['posted_at_key']: {
                    '$gte': start_time,
                    '$lt': end_time
                }
            }, timeout=False)
            print 'prod mode: 从mongodb加载[%s:%s]里的从%s开始50天的微博' % (self.mgdb, self.collection, datetime.datetime.fromtimestamp(start_time))
        elif mode == 'debug':
            with open("../test/sample_tweets.js") as f:
                weibos = json.loads(f.readline())
            print 'debug mode: 从测试数据文件中加载微博'

        self.weibos = weibos

    @timeit
    def index_weibos(self, start_time=None, mode='debug'):
        count = 0
        try:
            for weibo in self.weibos:
                count += 1
                if mode == 'single_given_db':
                    if not start_time:
                        raise Exception('single_given_db mode 需要指定start_time')
                    folder = self.ts_and_dbfolders[0][1]
                else:
                    posted_at = weibo[self.schema['posted_at_key']]
                    for i in xrange(len(self.ts_and_dbfolders) - 1):
                        if self.ts_and_dbfolders[i][0] <= posted_at < self.ts_and_dbfolders[i + 1][0]:
                            folder = self.ts_and_dbfolders[i][1]
                            break
                    else:
                        if posted_at >= self.ts_and_dbfolders[i + 1][0]:
                            folder = self.ts_and_dbfolders[i + 1][1]

                self.update(folder, weibo)
                if count % PROCESS_IDX_SIZE == 0:
                    print '[%s] folder[%s] num indexed: %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), folder, count)
        except Exception:
            raise

        finally:
            for database in self.databases.itervalues():
                database.close()

            for _, folder in self.ts_and_dbfolders:
                print '[', folder, ']', 'total size', self.document_count(folder)

    def update(self, folder, weibo):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + weibo[self.schema['obj_id']]
        for field in self.schema['idx_fields']:
            self.index_field(field, document, weibo, SCHEMA_VERSION)
        if 'dumps_exclude' in self.schema:
            for k in self.schema['dumps_exclude']:
                if k in weibo:
                    del weibo[k]
        document.set_data(json.dumps(weibo))
        document.add_term(document_id)
        self.get_database(folder).replace_document(document_id, document)
        #self.get_database(folder).add_document(document)

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
                          if 3 < len(token[0]) < 10 or token[0] in single_word_whitelist]
                termgen = xapian.TermGenerator()
                termgen.set_document(document)
                termgen.index_text_without_positions(' '.join(tokens), 1, prefix)
                """
                for token, count in Counter(tokens).iteritems():
                    document.add_term(prefix + token, count)
                """


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
            ``query_dict`` -- A query dict similar to MongoDB style to parse

        Returns a xapian.Query

        Operator Reference:
            Comparison:
            equal, key = value, { key:value }

            $lt, $gt, the field less or more than the specified value, { field: { $lt: value, $gt: value } }

            Logical:
            $and, perform logical AND operation in expressions,  { $and: [{ <expression1> } , { <expression2> },
                                                                            ... , { <expressionN> }] }

            $or, perform logical OR operation in expressions like the $and operation

            $xor, perform logical XOR operation in expressions like the $and operation

            $not, perform logical NOT operation in experssions, which get the conjunction of both negative
                  experssions, { $not: { <expression1> }, { <expression2> }, ...  { <expressionN> } }

            PS: if not any operation is specified, the logical AND operation is the default operation
            (An implicit AND operation is performed when specifying a comma separated list of expressions).
                See more query examples in test files.
        """
        if query_dict is None:
            return xapian.Query('')  # Match everything
        elif query_dict == {}:
            return xapian.Query()  # Match nothing

        query_tree = self.build_query_tree(query_dict)

        return query_tree.to_query(self.schema, self.database)

    def build_query_tree(self, query_dict):
        """将字典转成语法树"""
        ops = ['$not']
        bi_ops = ['$or', '$and', '$xor']

        def op(a, b, operation):
            if operation == '$and':
                return a & b
            elif operation == '$or':
                return a | b
            elif operation == '$xor':
                return a ^ b
            else:
                raise OperationError('Operation %s cannot be processed.' % operation)

        def grammar_tree(query_dict):
            total_query = Q()
            for k in query_dict.keys():
                if k in bi_ops:
                    #deal with expression without operator
                    bi_query = reduce(lambda a, b: op(a, b, k),
                                      map(lambda expr: Q(**expr),
                                          filter(lambda expr: not (set(expr.keys()) & set(ops + bi_ops)), query_dict[k])), Q())
                    #deal with nested expression
                    nested_query = reduce(lambda a, b: op(a, b, k),
                                          map(lambda nested_query_dict: grammar_tree(nested_query_dict),
                                              filter(lambda expr: set(expr.keys()) & set(ops + bi_ops), query_dict[k])), Q())
                    if nested_query:
                        total_query &= op(bi_query, nested_query, k)
                    else:
                        total_query &= bi_query

                elif k in ops:
                    if k == '$not':
                        not_dict = {}
                        #nested_query_dict = {}
                        for not_k in query_dict[k]:
                            if not_k not in ops + bi_ops:
                                not_dict[not_k] = query_dict[k][not_k]
                            else:
                                pass
                                #nested query in a $not statement is not implemented
                                #nested_query_dict.update({not_k: query_dict[k][not_k]})
                        not_query = notQ(**not_dict)
                        total_query &= not_query

                else:
                    total_query &= Q(**{k: query_dict[k]})
            return total_query

        total_query = grammar_tree(query_dict)

        return total_query

    def search(self, query=None, sort_by=None, start_offset=0,
               max_offset=1000, fields=None, **kwargs):

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

        if not max_offset:
            max_offset = database.get_doccount() - start_offset

        matches = self._get_enquire_mset(database, enquire, start_offset, max_offset)

        for match in matches:
            weibo = json.loads(self._get_document_data(database, match.document))
            item = None
            if fields is not None:  # 如果fields为[], 这情况下，不返回任何一项
                item = {}
                for field in fields:
                    item[field] = weibo.get(field, None)
            else:
                item = weibo
            results.append(item)

        return {
            'results': results,
            'hits': self._get_hit_count(database, enquire)
        }

    def _get_enquire_mset(self, database, enquire, start_offset, max_offset):
        """
        A safer version of Xapian.enquire.get_mset

        Simply wraps the Xapian version and catches any `Xapian.DatabaseModifiedError`,
        attempting a `database.reopen` as needed.

        Required arguments:
            `database` -- The database to be read
            `enquire` -- An instance of an Xapian.enquire object
            `start_offset` -- The start offset to pass to `enquire.get_mset`
            `max_offset` -- The max offset (maxitems to acquire) to pass to `enquire.get_mset`
        """
        try:
            return enquire.get_mset(start_offset, max_offset)
        except xapian.DatabaseModifiedError:
            database.reopen()
            return enquire.get_mset(start_offset, max_offset)

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


def _marshal_value(value, prefunc=None):
    """
    Private utility method that converts Python values to a string for Xapian values.
    prefunc 对值做预处理
    """
    if prefunc:
        value = prefunc(value)
    if isinstance(value, (int, long, float)):
        value = xapian.sortable_serialise(value)
    elif isinstance(value, bool):
        value = 1 if value else 0
        value = xapian.sortable_serialise(value)
    value = str(value).lower()
    return value


def _marshal_term(term):
    """
    Private utility method that converts Python terms to a string for Xapian terms.
    """
    if isinstance(term, int):
        term = str(term).lower()
    return term


def _database(folder, writable=False, refresh=False):
    """
    Private method that returns a xapian.Database for use.

    Optional arguments:
        ``writable`` -- Open the database in read/write mode (default=False)

    Returns an instance of a xapian.Database or xapian.WritableDatabase
    """
    if writable:
        if refresh:
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


class OperationError(Exception):
    """Raised when queries cannot be operated."""
    pass


class Schema:
    v1 = {
        'db': 'weibo',
        'collection': 'statuses',
        'dumps_exclude': ['_id', '_keywords', 'hashtags', '_md5', 'emotions', 'urls'],
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
    cd data/
    then run 'py (-m memory_profiler) ../xapian_weibo/xapian_backend.py -d hehe'
    http://pypi.python.org/pypi/memory_profiler
    """
    parser = ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='DEBUG')
    parser.add_argument('-a', '--idx_all', action='store_true', help='INDEX WHOLE COLLECTION')
    parser.add_argument('-p', '--print_folders', action='store_true', help='PRINT FOLDER THEN EXIT')
    parser.add_argument('-s', '--start_time', nargs=1, help='DATETIME')
    parser.add_argument('dbpath', help='PATH_TO_DATABASE')

    args = parser.parse_args(sys.argv[1:])
    debug = args.debug
    idx_all = args.idx_all
    start_time = args.start_time[0] if args.start_time else None
    dbpath = args.dbpath

    if args.print_folders:
        xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION)
        xapian_indexer.generate()
        for _, folder in xapian_indexer.ts_and_dbfolders:
            print folder

        sys.exit(0)

    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION, refresh_db=debug)
    xapian_indexer.generate(start_time)
    if debug:
        print 'debug mode'
        mode = 'debug'
    elif start_time:
        print 'single given db mode'
        mode = 'single_given_db'
    elif idx_all:
        print 'idx all collection to multi db mode'
        mode = 'idx_all'

    xapian_indexer.load_weibos(start_time, mode)
    xapian_indexer.index_weibos(start_time, mode)

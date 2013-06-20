#!/usr/bin/env python
# -*- coding: utf-8 -*-

from query_base import Q, notQ
from utils import load_scws, cut
from utils4scrapy.utils import local2unix
from xapian_weibo.utils import timeit
import os
import xapian
import msgpack


PROCESS_IDX_SIZE = 20000
SCHEMA_VERSION = 2
DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017

s = load_scws()


class Schema:
    v2 = {
        'db': 'master_timeline',
        'collection': 'master_timeline_weibo',
        'origin_data_iter_keys': ['_id', 'user', 'retweeted_status', 'text', 'timestamp', 'reposts_count', 'source', 'bmiddle_pic', 'geo'],
        'index_item_iter_keys': ['retweeted_status', 'user'],
        'index_value_iter_keys': ['_id', 'timestamp', 'reposts_count'],
        'pre_func': {
            'user': lambda x: x['id'] if x else 0,
            'retweeted_status': lambda x: x['id'] if x else 0,
        },
        'obj_id': '_id',
        # 用于去重的value no(column)
        'collapse_valueno': 3,
        'idx_fields': [
            # term
            {'field_name': 'user', 'column': 0, 'type': 'long'},
            {'field_name': 'retweeted_status', 'column': 1, 'type': 'long'},
            {'field_name': 'text', 'column': 2, 'type': 'text'},
            # value
            {'field_name': '_id', 'column': 3, 'type': 'long'},
            {'field_name': 'timestamp', 'column': 4, 'type': 'long'},
            {'field_name': 'reposts_count', 'column': 5, 'type': 'long'},
        ],
    }

    v1 = {
        'db': 'master_timeline',
        'collection': 'master_timeline_user',
        'origin_data_iter_keys': ['_id', 'province', 'city', 'verified', 'name', 'friends_count',
                                  'bi_followers_count', 'gender', 'profile_image_url', 'verified_reason', 'verified_type',
                                  'followers_count', 'followers', 'location', 'active', 'statuses_count', 'friends', 'description', 'created_at'],
        'index_item_iter_keys': ['name', 'location', 'province'],
        'index_value_iter_keys': ['_id', 'created_at', 'followers_count', 'statuses_count', 'friends_count', 'bi_followers_count'],
        'pre_func': {
            'created_at': lambda x: local2unix(x) if x else 0,
        },
        'obj_id': '_id',
        # 用于去重的value no(column)
        'collapse_valueno': 3,
        'idx_fields': [
            # term
            {'field_name': 'name', 'column': 0, 'type': 'term'},
            {'field_name': 'location', 'column': 1, 'type': 'term'},
            {'field_name': 'province', 'column': 2, 'type': 'term'},
            # value
            {'field_name': '_id', 'column': 3, 'type': 'long'},
            {'field_name': 'followers_count', 'column': 4, 'type': 'long'},
            {'field_name': 'statuses_count', 'column': 5, 'type': 'long'},
            {'field_name': 'friends_count', 'column': 6, 'type': 'long'},
            {'field_name': 'bi_followers_count', 'column': 7, 'type': 'long'},
            {'field_name': 'created_at', 'column': 8, 'type': 'long'},
        ],
    }

    v3 = {
        'origin_data_iter_keys': [],
        'index_item_iter_keys': ['user', 'sentiment'],
        'index_value_iter_keys': ['_id', 'timestamp'],
        'obj_id': '_id',
        # 用于去重的value no(column)
        'collapse_valueno': 3,
        'idx_fields': [
            # term
            {'field_name': 'user', 'column': 0, 'type': 'long'},
            {'field_name': 'text', 'column': 1, 'type': 'text'},
            {'field_name': 'sentiment', 'column': 2, 'type': 'int'},
            # value
            {'field_name': '_id', 'column': 3, 'type': 'long'},
            {'field_name': 'timestamp', 'column': 4, 'type': 'long'},
        ],
    }


class XapianSearch(object):
    def __init__(self, path, name='master_timeline_weibo', schema=Schema, schema_version=SCHEMA_VERSION):
        def create(dbpath):
            return _database(dbpath)

        def merge(db1, db2):
            db1.add_database(db2)
            return db1

        self.database = reduce(merge,
                               map(create,
                                   [os.path.join(path, p) for p in os.listdir(path) if p.startswith('_%s' % name)]))

        self.schema = getattr(schema, 'v%s' % schema_version)

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
               max_offset=None, fields=None, count_only=False, **kwargs):

        query = self.parse_query(query)

        if xapian.Query.empty(query):
            return 0, lambda: []

        database = self.database
        enquire = xapian.Enquire(database)
        enquire.set_weighting_scheme(xapian.BoolWeight())  # 使用最简单的weight模型提升效率
        enquire.set_docid_order(xapian.Enquire.DONT_CARE)  # 不关心mset的顺序
        enquire.set_query(query)

        if 'collapse_valueno' in self.schema:
            enquire.set_collapse_key(self.schema['collapse_valueno'])

        if count_only:
            return self._get_hit_count(database, enquire)

        if sort_by:
            self._set_sort_by(enquire, sort_by)

        if not max_offset:
            max_offset = database.get_doccount() - start_offset

        mset = self._get_enquire_mset(database, enquire, start_offset, max_offset)
        mset.fetch()  # 提前fetch，加快remote访问速度

        def result_generator():
            if fields is not None and set(fields) <= set(['_id', 'terms']):
                for match in mset:
                    item = {}
                    if '_id' in fields:
                        item['_id'] = match.docid
                    if 'terms' in fields:
                        item['terms'] = {term.term[5:]: term.wdf for term in match.document.termlist() if term.term.startswith('XTEXT')}
                    yield item
            else:
                for match in mset:
                    r = msgpack.unpackb(self._get_document_data(database, match.document))
                    if fields is not None:  # 如果fields为[], 这情况下，不返回任何一项
                        item = {}
                        for field in fields:
                            if field == 'terms':
                                item['terms'] = {term.term[5:]: term.wdf for term in match.document.termlist() if term.term.startswith('XTEXT')}
                            else:
                                item[field] = r.get(field)
                    else:
                        item = r
                    yield item

        return mset.size(), result_generator

    def _set_sort_by(self, enquire, sort_by):
        sorter = xapian.MultiValueKeyMaker()

        for sort_field in sort_by:
            if sort_field.startswith('-'):
                reverse = True
                sort_field = sort_field[1:]  # Strip the '-'
            else:
                reverse = False  # Reverse is inverted in Xapian -- http://trac.xapian.org/ticket/311
            sorter.add_value(self._value_column(sort_field), reverse)

        enquire.set_sort_by_key(sorter)

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
        raise ValueError('Field %s cannot be used in sort_by clause' % field)

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

    def _get_document_ids_terms(self, mset, fields):
        ids = []
        terms = {}
        mset.fetch()  # 提前fetch，加快remote访问速度
        for match in mset:
            ids.append(match.docid)
            if 'terms' in fields:
                terms[match.docid] = {term.term[5:]: term.wdf for term in match.document.termlist() if term.term.startswith('XTEXT')}

        return ids, terms


def _marshal_value(value, pre_func=None):
    """
    Private utility method that converts Python values to a string for Xapian values.
    """
    if pre_func:
        value = pre_func(value)
    # value 默认为int, long, float
    value = xapian.sortable_serialise(value)
    return value


def _marshal_term(term, pre_func=None):
    """
    Private utility method that converts Python terms to a string for Xapian terms.
    """
    if pre_func:
        term = pre_func(term)
    if isinstance(term, (int, long)):
        term = str(term)
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


def _index_field(field, document, item, schema_version, schema, termgen):
    prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field['field_name'].upper()
    field_name = field['field_name']
    # 可选term在pre_func里处理
    if field_name in schema['index_item_iter_keys']:
        term = _marshal_term(item.get(field_name), schema['pre_func'].get(field_name))
        document.add_term(prefix + term)
    # 可选value在pre_func里处理
    elif field_name in schema['index_value_iter_keys']:
        value = _marshal_value(item.get(field_name), schema['pre_func'].get(field_name))
        document.add_value(field['column'], value)
    elif field_name == 'text':
        text = item['text'].encode('utf-8')
        tokens = cut(s, text)
        termgen.set_document(document)
        termgen.index_text_without_positions(' '.join(tokens), 1, prefix)

class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass


class OperationError(Exception):
    """Raised when queries cannot be operated."""
    pass

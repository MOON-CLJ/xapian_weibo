#!/usr/bin/env python
# -*- coding: utf-8 -*-

from query_base import Q, notQ
from utils import load_scws, cut
from utils4scrapy.utils import local2unix
import os
import xapian
import msgpack
import time


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
            'geo': lambda x: msgpack.packb(x) if x else None,
        },
        'obj_id': '_id',
        # 用于去重的value no(column)
        'collapse_valueno': 3,
        'posted_at_key': 'timestamp',
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
        'index_iter_keys': [],
        'dumps_exclude': ['id', 'first_in', 'last_modify'],
        'pre': {
            'created_at': lambda x: local2unix(x)
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


class XapianSearch(object):
    def __init__(self, path, name='master_timeline_weibo', schema=Schema, schema_version=SCHEMA_VERSION):
        def create(dbpath):
            return xapian.Database(dbpath)

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
               max_offset=None, fields=None, **kwargs):

        query = self.parse_query(query)

        if xapian.Query.empty(query):
            return 0, lambda: []

        database = self.database
        enquire = xapian.Enquire(database)
        enquire.set_query(query)
        if 'collapse_valueno' in self.schema:
            enquire.set_collapse_key(self.schema['collapse_valueno'])

        if sort_by:
            sorter = xapian.MultiValueSorter()

            for sort_field in sort_by:
                if sort_field.startswith('-'):
                    reverse = True
                    sort_field = sort_field[1:]  # Strip the '-'
                else:
                    reverse = False  # Reverse is inverted in Xapian -- http://trac.xapian.org/ticket/311
                sorter.add(self._value_column(sort_field), reverse)

            enquire.set_sort_by_key(sorter, True)

        if not max_offset:
            max_offset = database.get_doccount() - start_offset

        matches = self._get_enquire_mset(database, enquire, start_offset, max_offset)

        def result_generator():
            for match in matches:
                r = msgpack.unpackb(self._get_document_data(database, match.document))
                if fields is not None:  # 如果fields为[], 这情况下，不返回任何一项
                    item = {}
                    if isinstance(fields, list):
                        for field in fields:
                            if field == 'terms':
                                item['terms'] = dict([(term.term[5:], term.wdf) for term in match.document.termlist() if term.term.startswith('XTEXT')])
                            else:
                                item[field] = r.get(field)
                else:
                    item = r
                yield item

        return self._get_hit_count(database, enquire), result_generator

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
    if schema_version == 2:
        # 可选term在pre_func里处理
        if field['field_name'] in schema['index_item_iter_keys']:
            term = _marshal_term(item.get(field['field_name']), schema['pre_func'][field['field_name']])
            document.add_term(prefix + term)
        # value
        elif field['field_name'] in schema['index_value_iter_keys']:
            value = _marshal_value(item.get(field['field_name']))
            document.add_value(field['column'], value)
        elif field['field_name'] == 'text':
            text = item['text'].encode('utf-8')
            tokens = cut(s, text)
            termgen.set_document(document)
            termgen.index_text_without_positions(' '.join(tokens), 1, prefix)

    elif schema_version == 1:
        # 必选term
        if field['field_name'] in ['name', 'location', 'province']:
            term = _marshal_term(item[field['field_name']])
            document.add_term(prefix + term)
        # 可选value
        elif field['field_name'] in ['created_at']:
            value = _marshal_value(item[field['field_name']], schema['pre'][field['field_name']]) if field['field_name'] in item else '0'
            document.add_value(field['column'], value)
        # 必选value
        elif field['field_name'] in ['_id', 'followers_count', 'statuses_count', 'friends_count', 'bi_followers_count']:
            document.add_value(field['column'], _marshal_value(item[field['field_name']]))


class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass


class OperationError(Exception):
    """Raised when queries cannot be operated."""
    pass

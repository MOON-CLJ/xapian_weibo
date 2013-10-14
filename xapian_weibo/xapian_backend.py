#!/usr/bin/env python
# -*- coding: utf-8 -*-

from consts import XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION, XAPIAN_REMOTE_OPEN_TIMEOUT
from query_base import parse_query
from utils import local2unix
import os
import xapian
import msgpack


SCHEMA_VERSION = XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION

DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'

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
            {'field_name': 'gender', 'column': 9, 'type': 'term'},
            {'field_name': 'verified_type', 'column': 10, 'type': 'term'},
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
        'origin_data_iter_keys': ['_id'],
        'index_item_iter_keys': ['user', 'sentiment'],
        'index_value_iter_keys': ['_id', 'timestamp', 'reposts_count'],
        'pre_func': {
            'user': lambda x: x['id'] if x else 0,
        },
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
            {'field_name': 'reposts_count', 'column': 5, 'type': 'long'},
        ],
    }


def fields_not_empty(func):
    def _(*args, **kwargs):
        fields = kwargs.get('fields')
        if fields == []:
            raise ValueError('fields should not be empty list')
        return func(*args, **kwargs)
    return _


class XapianSearch(object):
    def __init__(self, path=None, name='master_timeline_weibo', stub=None, include_remote=False, schema=Schema, schema_version=SCHEMA_VERSION):
        def create(dbpath):
            return _database(dbpath)

        def merge(db1, db2):
            db1.add_database(db2)
            return db1

        if stub:
            if os.path.isfile(stub):
                self.database = _stub_database(stub)
            elif os.path.isdir(stub):
                self.database = reduce(merge,
                                       map(_stub_database, [p for p in os.listdir(stub)]))
        else:
            self.database = reduce(merge,
                                   map(create, [os.path.join(path, p) for p in os.listdir(path) if p.startswith('_%s' % name)]))

        self.schema = getattr(schema, 'v%s' % schema_version)
        enquire = xapian.Enquire(self.database)
        enquire.set_weighting_scheme(xapian.BoolWeight())  # 使用最简单的weight模型提升效率
        enquire.set_docid_order(xapian.Enquire.DONT_CARE)  # 不关心mset的顺序

        if 'collapse_valueno' in self.schema:
            enquire.set_collapse_key(self.schema['collapse_valueno'])

        self.enquire = enquire
        self.include_remote = include_remote

    @fields_not_empty
    def iter_all_docs(self, fields=None):
        db = self.database
        match_all = ""
        postlist = db.postlist(match_all)
        while 1:
            try:
                plitem = postlist.next()
            except StopIteration:
                break

            doc = db.get_document(plitem.docid)
            if fields == ['terms']:
                item = {}
                item['terms'] = {term.term[5:]: term.wdf for term in doc.termlist() if term.term.startswith('XTEXT')}
                yield item
            else:
                r = msgpack.unpackb(self._get_document_data(db, doc))
                if fields is not None:
                    item = {}
                    for field in fields:
                        if field == 'terms':
                            item['terms'] = {term.term[5:]: term.wdf for term in doc.termlist() if term.term.startswith('XTEXT')}
                        else:
                            item[field] = r.get(field)
                else:
                    item = r
                yield item

    def iter_all_xapian_terms(self, field):
        db = self.database
        if field == '_id':
            prefix = DOCUMENT_ID_TERM_PREFIX
        else:
            prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field.upper()

        term_iter = db.allterms_begin(prefix)
        while term_iter != db.allterms_end(prefix):
            term = term_iter.get_term()
            yield term.lstrip(prefix)
            term_iter.next()

    @fields_not_empty
    def search_by_id(self, id_, fields=None):
        db = self.database
        postlist = db.postlist(DOCUMENT_ID_TERM_PREFIX + str(id_))
        try:
            plitem = postlist.next()
        except StopIteration:
            return

        doc = db.get_document(plitem.docid)
        if fields == ['terms']:
            item = {}
            item['terms'] = {term.term[5:]: term.wdf for term in doc.termlist() if term.term.startswith('XTEXT')}
            return item
        else:
            r = msgpack.unpackb(self._get_document_data(db, doc))
            if fields is not None:
                item = {}
                for field in fields:
                    if field == 'terms':
                        item['terms'] = {term.term[5:]: term.wdf for term in doc.termlist() if term.term.startswith('XTEXT')}
                    else:
                        item[field] = r.get(field)
            else:
                item = r
            return item

    @fields_not_empty
    def search(self, query=None, sort_by=None, start_offset=0,
               max_offset=None, fields=None, count_only=False, **kwargs):

        db = self.database
        enquire = self.enquire

        query = parse_query(query, self.schema, db)
        if xapian.Query.empty(query):
            return 0, lambda: []

        enquire.set_query(query)

        if count_only:
            return self._get_hit_count(db, enquire)

        if sort_by:
            self._set_sort_by(enquire, sort_by, self.include_remote)

        if not max_offset:
            max_offset = db.get_doccount() - start_offset

        mset = self._get_enquire_mset(db, enquire, start_offset, max_offset)
        mset.fetch()  # 提前fetch，加快remote访问速度

        def result_generator():
            if fields == ['terms']:
                for match in mset:
                    item = {}
                    item['terms'] = {term.term[5:]: term.wdf for term in match.document.termlist() if term.term.startswith('XTEXT')}
                    yield item
            else:
                for match in mset:
                    r = msgpack.unpackb(self._get_document_data(db, match.document))
                    if fields is not None:
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

    def _set_sort_by(self, enquire, sort_by, remote=False):
        if remote:
            for sort_field in sort_by:
                if sort_field.startswith('-'):
                    reverse = False
                    sort_field = sort_field[1:]  # Strip the '-'
                else:
                    reverse = True
                enquire.set_sort_by_value(self._value_column(sort_field), reverse)
        else:
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
            raise InvalidIndexError(u'Unable to open database at %s' % folder)

    return database


def _stub_database(stub):
    f = open(stub, 'U')
    dbpaths = f.readlines()
    f.close()
    if not dbpaths[0].startswith('remote'):
        # local database
        database = xapian.open_stub(stub)
        return database

    dbpaths = [p.lstrip('remote ssh ') for p in dbpaths]

    def create(dbpath):
        return xapian.remote_open('ssh', dbpath, XAPIAN_REMOTE_OPEN_TIMEOUT)

    def merge(db1, db2):
        db1.add_database(db2)
        return db1

    database = reduce(merge,
                      map(create, [p for p in dbpaths]))
    return database


class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass

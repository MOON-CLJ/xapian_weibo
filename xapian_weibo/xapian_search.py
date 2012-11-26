# -*- coding:utf-8 -*-

import sys
import os

import cPickle as pickle

import xapian


DOCUMENT_CUSTOM_TERM_PREFIX = 'X'


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


class Search(object):
    def __init__(self, path='../test/', name='haha'):
        def creat(dbpath):
            return xapian.Database(dbpath)

        def merge(db1, db2):
            db1.add_database(db2)
            return db1

        self.database = reduce(merge,
                               map(creat, 
                                   [path+p for p in os.listdir(path) if p.startswith('_%s' % name)]))

        self.schema = Schema.v1
        print 'totally indexed %s doc.' % self.database.get_doccount()
    
    def parse_query(self, query_dict):
        """
        Given a `query_dict`, will attempt to return a xapian.Query

        Required arguments:
            ``query_dict`` -- A MongoDB style query dict to parse

        Returns a xapian.Query
        """
        if query_dict == None:
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
                        begin = None
                        end = None
                        if "$gt" in value:
                            begin = value['$gt']
                        if "$lt" in value:
                            end = value['$lt']
                        if not begin:
                            begin = 0
                        if not end:
                            end = sys.maxint
                        qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(col, '%s' % prefix))
                        new_query = qp.parse_query('%s%s..%s' % (prefix, begin, end))
                    else:
                        pass
                elif not hasattr(value, "strip") and hasattr(value, "__getitem__") or hasattr(value, "__iter__"):
                    value = ['%s%s' % (prefix, v) for v in value]
                    new_query = xapian.Query(xapian.Query.OP_AND, value)
                else:
                    new_query = xapian.Query('%s%s' % (prefix, value))
                query = xapian.Query(xapian.Query.OP_AND, [query, new_query])
            else:
                continue
        return query

    def search(self, query=None, sort_by=None, start_offset=0,
               end_offset=10, fields=None, **kwargs):

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
            item = {}
            for field in fields:
                item[field] = weibo[field]
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


def main():
    s = Search()
    results = s.search(query={'text': [u'中国'], 'uid': 1217743083, 'ts': {'$gt': 0, '$lt': 1334450340}}, sort_by=['-ts'], fields=['text', 'ts'])
    for r in results['results']:
        print r

if __name__ == '__main__': main()

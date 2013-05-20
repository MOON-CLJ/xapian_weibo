#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import xapian


OPERATIONINT2STR = {
    '0': 'AND',
    '1': 'AND_NOT',
    '2': 'OR',
    '3': 'XOR',
    '4': 'NOT',
}


DOCUMENT_ID_TERM_PREFIX = 'M'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'


class QNodeVisitor(object):
    """
    Base visitor class for visiting Q-object nodes in a query tree.
    """

    def visit_combination(self, combination):
        """
        Called by QCombination objects.
        """
        return combination

    def visit_query(self, query):
        """
        Called by (New)Q objects.
        """
        return query

    def visit_not_query(self, query):
        """
        Called by (New)NOT Q objects.
        """
        return query


class QueryCompilerVisitor(QNodeVisitor):
    """
    Compiles the nodes in a query tree to a Xapian-compatible query.
    """

    def __init__(self, schema, database):
        self.schema = schema
        self.database = database

    def visit_combination(self, combination):
        if combination.operation == combination.OR:
            return xapian.Query(xapian.Query.OP_OR, combination.children)
        elif combination.operation == combination.AND:
            return xapian.Query(xapian.Query.OP_AND, combination.children)
        elif combination.operation == combination.AND_NOT:
            return xapian.Query(xapian.Query.OP_AND_NOT, combination.children)
        elif combination.operation == combination.XOR:
            return xapian.Query(xapian.Query.OP_XOR, combination.children)
        return combination

    def visit_not_query(self, query):
        new_query = self.visit_query(query, n=True)
        #NOT set is the intersection of universal set AND NOT set
        new_query = xapian.Query(xapian.Query.OP_AND_NOT, [xapian.Query(''), new_query])
        return new_query

    def visit_query(self, query, n=False):
        query_dict = query.query

        qp = xapian.QueryParser()
        qp.set_database(self.database)

        field_prefix = {}
        field_type = {}
        field_col = {}

        for field_dict in self.schema['idx_fields']:
            fname = field_dict['field_name']
            field_col[fname] = field_dict['column']
            field_type[fname] = field_dict['type']
            field_prefix[fname] = DOCUMENT_CUSTOM_TERM_PREFIX + fname.upper()
        field_prefix[self.schema['obj_id']] = DOCUMENT_ID_TERM_PREFIX
        pre_query = None
        new_query = None
        for field in query_dict:
            if field in field_prefix:
                prefix = field_prefix[field]
                col = field_col.get(field)
                value = query_dict[field]

                if isinstance(value, dict):
                    ftype = field_type[field]
                    if ftype == 'int' or ftype == 'long':
                        begin = value.get('$gt', 0)
                        end = value.get('$lt', sys.maxint)
                        qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(col, prefix))
                        new_query = qp.parse_query('%s%s..%s' % (prefix, begin, end))
                elif not isinstance(value, basestring) and hasattr(value, '__getitem__') or hasattr(value, '__iter__'):
                    value = ['%s%s' % (prefix, v) for v in value]
                    #De Morgan's laws, if we want the intersection of negation sets,
                    #Firstly, we obtain the disjunction of this sets, then get negation of them
                    # (AND_NOT [U, (OR, [a, b, c])])
                    # NOT (a OR B OR C)
                    # NOT a AND not b AND not C
                    if not n:
                        new_query = xapian.Query(xapian.Query.OP_AND, value)
                    else:
                        new_query = xapian.Query(xapian.Query.OP_OR, value)
                else:
                    new_query = xapian.Query('%s%s' % (prefix, value))
                if pre_query:
                    if not n:
                        new_query = xapian.Query(xapian.Query.OP_AND, [pre_query, new_query])
                    else:
                        # and_not , [U, a or b])
                        # not a and not b
                        new_query = xapian.Query(xapian.Query.OP_OR, [pre_query, new_query])
                pre_query = new_query

        return new_query


class QNode(object):
    """
    Base class for nodes in query trees.
    """

    AND = 0
    AND_NOT = 1
    OR = 2
    XOR = 3
    NOT = 4

    def to_query(self, schema, database):
        query = self.accept(QueryCompilerVisitor(schema, database))
        return query

    def accept(self, visitor):
        """在to_query里被调用，不同子类有不同实现"""
        raise NotImplementedError

    def _combine(self, other, operation):
        """
        Combine this node with another node into a QCombination object.
        """
        if getattr(other, 'empty'):
            return self

        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)

    def __xor__(self, other):
        return self._combine(other, self.XOR)


class QCombination(QNode):
    """
    Represents the combination of several conditions by a given logical
    operator.
    """

    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def accept(self, visitor):
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)
        return visitor.visit_combination(self)

    @property
    def empty(self):
        return not self.children

    def __repr__(self):
        return '%s: (%s, [%s])' % \
            (type(self), OPERATIONINT2STR[str(self.operation)], ', '.join([str(x) for x in self.children]))


class Q(QNode):
    """
    A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def accept(self, visitor):
        return visitor.visit_query(self)

    @property
    def empty(self):
        return not self.query

    def __repr__(self):
        return '%s: %s' % (type(self), self.query)


class notQ(Q):
    """
    A query object based on simple query object, used in a query tree to
    build up NOT query structures.
    """
    def __init__(self, **query):
        self.query = query

    def accept(self, visitor):
        return visitor.visit_not_query(self)


class InvalidQueryError(Exception):
    """Raised when a query is illegal."""
    pass

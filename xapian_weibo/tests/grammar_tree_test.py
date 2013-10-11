# -*- coding:utf-8 -*-

import sys

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

s = XapianSearch(path='../data/', name='statuses')

query_dict1 = {
    '$and': [{'text': '1',
              'uid': '2'}],
    '$not': {
        'name': '3',
        'text': '4',
    },
    'name': '5',
}

print s.build_query_tree(query_dict1)
print s.parse_query(query_dict1)

query_dict2 = {
    '$and': [{'text': '1', 'ts': {'$gt': 0, '$lt': 1}},
             {'$or': [{'uid': 3},
                      {'uid': 4}]}],
    '$not': {
        'name': '3',
        'text': '4',
    },
    'name': '5',
}

print s.build_query_tree(query_dict2)
print s.parse_query(query_dict2)

query_dict3 = {
    '$or': [{'text': '1'},
            {'$and': [{'uid': 3},
                      {'uid': 4}]}],
    '$and': [{'text': 3},
             {'ts': 5}],
    '$not': {
        'name': '3',
        'text': '4',
    },
    'name': '5',
}

print s.build_query_tree(query_dict3)
print s.parse_query(query_dict3)

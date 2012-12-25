# -*- coding:utf-8 -*-

import sys

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

s = XapianSearch(path='../data/', name='hehe')

query_dict1 = {
    '$and': {
        'text': '1',
        'uid': '2',
    },
    '$not': {
        'name': '3',
        'text': '4',
    },
    'name': '5',
}

#print s.build_query_tree(query_dict1)
#print s.parse_query(query_dict1)

query_dict2 = {
    '$and': {
        'text': '1',
        '$and': {
            'uid': 3,
            'text': 4,
        }
    },
    '$not': {
        'name': '3',
        'text': '4',
    },
    'name': '5',
}

print s.build_query_tree(query_dict2)
print s.parse_query(query_dict2)

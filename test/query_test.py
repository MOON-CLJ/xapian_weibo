# -*- coding:utf-8 -*-

import sys
import  calendar
import datetime

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch

s = XapianSearch(path='../data/', name='statuses')

"""
results = s.search(query={'text': [u'中国'], 'uid': 1217743083, 'ts': {'$gt': 0, '$lt': 1334450340}}, sort_by=['-ts'], fields=['text', 'ts', 'name'])

print 'query1:'

for r in results['results']:
    print r['ts']

print 'hits: %s' % results['hits']

print 'query2:'
query_dict = {'$and': {'text': [u'中国'],
                       'uid': 1217743083,
                       '$or': {'ts': {'gt': 0,
                                      'lt': 1334450340},
                               'uid': 0000000000}},
              '$not': {'text': u'宝马', 'name': u'白之兔'},
              'name': u'袁岳'
              }

results = s.search(query=query_dict, sort_by=['-ts'], fields=['text', 'ts'])

for r in results['results']:
    print r['text']

print 'hits: %s' % results['hits']
"""

begin_ts1 = calendar.timegm(datetime.datetime(2011, 10, 1).timetuple())
end_ts1 = calendar.timegm(datetime.datetime(2011, 12, 31).timetuple())
begin_ts2 = calendar.timegm(datetime.datetime(2010, 10, 1).timetuple())
end_ts2 = calendar.timegm(datetime.datetime(2010, 12, 31).timetuple())

query_dict = {
    '$or': {
        'ts': {
            '$gt': begin_ts1,
            '$lt': end_ts1,
        },
        'ts': {
            '$gt': begin_ts2,
            '$lt': end_ts2,
        },
    }
}
results = s.search(query=query_dict)
print results['hits']

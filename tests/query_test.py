# -*- coding:utf-8 -*-

import sys
import time
import datetime

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch
from utils import top_keywords, not_low_freq_keywords, gen_mset_iter

# 默认schema_version为2
s = XapianSearch(path='/opt/xapian_weibo/data/20131207/', name='master_timeline_weibo')

# import和初始化, 请使用下面的用法
# from xapian_weibo.xapian_backend import XapianSearch
# s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
# 查询条件有user(id),retweeted_status(id),text,timestamp,reposts_count,comments_count,attitudes_count(从timestamp开始后面四个查询指标可以指定范围和排序)
# 返回字段基本和新浪api的返回字段相同，注意没有created_at，而是timestamp
# 值得注意的是新增返回字段terms，返回的是每条微博里的词和以及词频的dict（字典），所有不用自己取出来之后再分词
# 若fields参数不指定，或者为None,则返回所有字段，除terms之外
# 如果需要返回terms，请一一指定需要的字段，并包括terms
# 简单示例如下

"""
count, get_results = s.search(query={'text': [u'中国'], 'user': 1217743083, 'timestamp': {'$gt': 0, '$lt': 1334450340}}, sort_by=['-timestamp'], fields=['text', 'timestamp', 'user', 'terms', '_id'])

print 'query1:'

for r in get_results():
    print "** " * 10
    print r['_id']
    print r['user']
    print r['text']
    print r['timestamp']
    print r['terms']

print 'hits: %s' % count

print 'query2:'
count, get_results = s.search(query={'_id': 72447122}, fields=['text', 'timestamp', 'user', 'terms', '_id'])
print count
for r in get_results():
    print "** " * 10
    print r['_id']
    print r['user']
    print r['text']
    print r['timestamp']
    print r['terms']

print 'query3:'
begin_ts1 = time.mktime(datetime.datetime(2012, 1, 1).timetuple())
end_ts1 = time.mktime(datetime.datetime(2013, 3, 1).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': end_ts1},
    '$not': {'retweeted_status': '0'}
}
count, get_results = s.search(query=query_dict, fields=['retweeted_status', 'user'])
print count
for r in get_results():
    if r['retweeted_status'] is None:
        print '** ' * 10
        print r
        break

print 'query4:'
begin_ts1 = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
end_ts1 = time.mktime(datetime.datetime(2013, 3, 1).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': end_ts1},
    '$not': {'retweeted_status': '0'}
}
count, get_results = s.search(query=query_dict, fields=['user'])
print count
uids = set()
for r in get_results():
    uids.add(r['user'])

print len(uids)

print 'query5:'
begin_ts1 = time.mktime(datetime.datetime(2013, 1, 1).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': begin_ts1 + 3600},
}
count, get_results = s.search(query=query_dict, fields=['terms'])
print count
print top_keywords(get_results, top=10)


print 'query6:'
r = s.search_by_id(3434992295856700, fields=['text', 'user', 'terms', '_id'])
print r['_id']
print r['text']
print r['user']
print r['terms']
"""

print 'query7:'
all_terms = s.iter_all_xapian_terms(field='text')
for term, termfreq in all_terms:
    if termfreq > 100:
        print term, termfreq

"""
print 'query8:'
begin_ts1 = time.mktime(datetime.datetime(2013, 1, 1).timetuple())

query_dict = {
    'timestamp': {'$gt': begin_ts1, '$lt': begin_ts1 + 600},
}
mset = s.search(query=query_dict, mset_direct=True)
print top_keywords(gen_mset_iter(s, mset, fields=['terms']), top=3)
print top_keywords(gen_mset_iter(s, mset, fields=['terms']), top=3)
"""


# 下面的用法由于接口的修改暂时没有维护, 但具有参考价值
"""
print 'query2:'
query_dict = {'$and': [{'text': [u'中国'], 'uid': 1217743083},
                       {'uid': 1217743083},
                       {'$or': [{'ts': {'gt': 0,
                                        'lt': 1334450340}},
                                {'uid': 0000000000}]}],
              '$not': {'text': u'宝马', 'name': u'白之兔'},
              'name': u'袁岳'
              }

results = s.search(query=query_dict, sort_by=['-ts'], fields=['text', 'ts', 'name'])

for r in results['results']:
    print r['text']
    print r['name']

print 'hits: %s' % results['hits']
print s.parse_query(query_dict)
"""
"""
begin_ts1 = time.mktime(datetime.datetime(2011, 10, 1).timetuple())
end_ts1 = time.mktime(datetime.datetime(2011, 12, 31).timetuple())
begin_ts2 = time.mktime(datetime.datetime(2010, 10, 1).timetuple())
end_ts2 = time.mktime(datetime.datetime(2010, 12, 31).timetuple())

query_dict = {'$or':
              [{'ts': {
                '$gt': begin_ts1,
                '$lt': end_ts1,
                }},
               {'ts': {
                '$gt': begin_ts2,
                '$lt': end_ts2,
                }}]}

results = s.search(query=query_dict, fields=['uid', 'text', 'ts', 'name', 'emotions'])
for r in results['results']:
    print r['emotions']
print results['hits']
"""

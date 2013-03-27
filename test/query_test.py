# -*- coding:utf-8 -*-

import sys
import  calendar
import datetime

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch
from utils import top_keywords, not_low_freq_keywords

# 默认schema_version为2
s = XapianSearch(path='../data/', name='master_timeline')

# import和初始化, 请使用下面的用法
# from xapian_weibo.xapian_backend import XapianSearch
# s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')
# 查询条件有user(id),retweeted_status(id),text,timestamp,reposts_count,comments_count,attitudes_count(从timestamp开始后面四个查询指标可以指定范围和排序)
# 返回字段基本和新浪api的返回字段相同，注意没有created_at，而是timestamp
# 值得注意的是新增返回字段terms，返回的是每条微博里的词和以及词频的dict（字典），所有不用自己取出来之后再分词
# 若fields参数不指定，或者为None,则返回所有字段，除terms之外
# 如果需要返回terms，请一一指定需要的字段，并包括terms
# 简单示例如下
count, get_results = s.search(query={'text': [u'中国'], 'user': 1217743083, 'timestamp': {'$gt': 0, '$lt': 1334450340}}, sort_by=['-timestamp'], fields=['text', 'timestamp', 'user', 'terms'])

print 'query1:'

for r in get_results():
    print "** " * 10
    print r['user']
    print r['text']
    print r['timestamp']
    print r['terms']

print 'hits: %s' % count


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
begin_ts1 = calendar.timegm(datetime.datetime(2011, 10, 1).timetuple())
end_ts1 = calendar.timegm(datetime.datetime(2011, 12, 31).timetuple())
begin_ts2 = calendar.timegm(datetime.datetime(2010, 10, 1).timetuple())
end_ts2 = calendar.timegm(datetime.datetime(2010, 12, 31).timetuple())

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

"""
begin_ts1 = calendar.timegm(datetime.datetime(2011, 10, 1).timetuple())
end_ts1 = calendar.timegm(datetime.datetime(2011, 12, 31).timetuple())
begin_ts2 = calendar.timegm(datetime.datetime(2010, 10, 1).timetuple())
end_ts2 = calendar.timegm(datetime.datetime(2010, 12, 31).timetuple())

query_dict = {'$or':
              [{'ts': {
                '$gt': begin_ts1,
                '$lt': end_ts1,
                }},
               {'ts': {
                '$gt': begin_ts2,
                '$lt': end_ts2,
                }}]}


for word, count in top_keywords(s, query_dict, emotions_only=True, top=1000):
    print word, count
"""
"""
begin_ts1 = calendar.timegm(datetime.datetime(2011, 1, 1).timetuple())
end_ts1 = calendar.timegm(datetime.datetime(2011, 12, 31).timetuple())
query_dict = {
    'ts': {
        '$gt': begin_ts1,
        '$lt': end_ts1,
    },
    'text': u'抓狂',
}

for word, count in top_keywords(s, query_dict, emotions_only=True, top=1000):
    print word, count

for word, count in not_low_freq_keywords(s, query_dict, emotions_only=True):
    print word, count
"""

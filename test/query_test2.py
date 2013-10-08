# -*- coding:utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch

'''
This test is for query via index of 'domain','sentiment','repost' in server 61
'''

'''
s = XapianSearch(path='/home/arthas/dev/xapian_weibo_taolei/xapian_weibo/data/20131007', name='master_timeline_repost',schema_version=5)
results = s.iter_all_docs(fields=['_id','_id_reposted'])
for r in results:
    print r
#count, get_results = s.search(query={'_id':''}, fields=['_id','_id_reposted'])
#for r in get_results():
#    print r
'''

'''
s = XapianSearch(path='/home/arthas/dev/xapian_weibo_taolei/xapian_weibo/data/20131007', name='master_timeline_sentiment',schema_version=3)
results = s.iter_all_docs(fields=['_id'])#only '_id' can be returned,others should be none
for r in results:
    print r
#count, get_results = s.search(query={'':'10'}, fields=['_id','domain'])
#for r in get_results():
#    print r
'''


s = XapianSearch(path='/home/arthas/dev/xapian_weibo_taolei/xapian_weibo/data/20131007', name='master_timeline_domain',schema_version=4)
results = s.iter_all_docs(fields=['_id','domain'])
for r in results:
    print r
#count, get_results = s.search(query={'domain':'10'}, fields=['_id','domain'])
#for r in get_results():
#    print r


'''
具体内容可参考xapian_weibo下的xapian_backend.py Class schema
'domain'索引能被用于查询的字段有'name', 'location', 'province', 'domain', '_id', 'created_at', 'followers_count', 'statuses_count', 'friends_count', 'bi_followers_count'
'repost'索引能被用于查询的字段有'user','text', '_uid_reposted', '_id', '_id_reposted', 'timestamp', 'reposts_count'
'sentiment'索引能用于查询的字段有'user', 'sentiment','text', '_id', 'timestamp', 'reposts_count'
'''

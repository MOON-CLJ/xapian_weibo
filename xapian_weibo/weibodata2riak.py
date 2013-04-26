# -*- coding: utf-8 -*-

from riak.client import RiakClient
from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import load_emotion_words
import riak
import datetime
import time
import opencc
import re

Nodes = [
    {'host': '219.224.135.60', 'pb_port': 10017, 'http_port': 10018},
    {'host': '219.224.135.60', 'pb_port': 10027, 'http_port': 10028},
    {'host': '219.224.135.60', 'pb_port': 10037, 'http_port': 10038},
    {'host': '219.224.135.60', 'pb_port': 10047, 'http_port': 10048},
    {'host': '219.224.135.60', 'pb_port': 10057, 'http_port': 10058},
]
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


@timeit
def load_weibos_from_xapian():
    begin_ts = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
    end_ts = time.mktime(datetime.datetime(2013, 5, 1).timetuple())

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }
    count, get_results = s.search(query=query_dict, fields=['id', 'retweeted_status', 'text'])
    print count
    return get_results


@timeit
def store2riak(get_results):
    cc = opencc.OpenCC('s2t')
    emotions_words = load_emotion_words()
    emotions_words = [unicode(e, 'utf-8') for e in emotions_words]
    t_emotions_words = [cc.convert(e) for e in emotions_words]
    emotions_words.extend(t_emotions_words)
    emotions_words_set = set(emotions_words)

    weibo_is_retweet_status_bucket = client.bucket('lijun_weibo_is_retweet_status')
    weibo_emoticoned = client.bucket('lijun_weibo_emoticoned')
    weibo_empty_retweet = client.bucket('lijun_weibo_empty_retweet')

    count = 0
    ts = te = time.time()
    for r in get_results():
        id_str = str(r['id'])

        while 1:
            try:
                # 微博是否为转发微博
                is_retweet_status = 1 if r['retweeted_status'] else 0
                new_node = weibo_is_retweet_status_bucket.new(id_str, data=is_retweet_status)
                new_node.store(return_body=False)
                break
            except riak.RiakError, e:
                if e.value == 'timeout':
                    print 'retry'
                else:
                    raise

        while 1:
            try:
                # 微博是否包含指定的表情符号集
                emotions = re.findall(r'\[(\S+?)\]', r['text'])
                is_emoticoned = 1 if set(emotions) & emotions_words_set else 0
                new_node = weibo_emoticoned.new(id_str, data=is_emoticoned)
                new_node.store(return_body=False)
                break
            except riak.RiakError, e:
                if e.value == 'timeout':
                    print 'retry'
                else:
                    raise

        while 1:
            try:
                # 是否为转发微博几个字
                is_empty_retweet = 1 if r['text'] in [u'转发微博', u'轉發微博', u'Repost'] else 0
                new_node = weibo_empty_retweet.new(id_str, data=is_empty_retweet)
                new_node.store(return_body=False)
                break
            except riak.RiakError, e:
                if e.value == 'timeout':
                    print 'retry'
                else:
                    raise

        count += 1
        if count % 3333 == 0:
            te = time.time()
            print '.', count, '%ssec' % (te - ts)
            ts = te


@timeit
def test_riak_write(test_bucket, n):
    for i in range(n):
        test_bucket.new(str(i), data=i).store()


@timeit
def test_riak_read(test_bucket, n):
    for i in range(n):
        r = test_bucket.get(str(i))
        # stable version
        # data = r.get_data()
        # master_version
        data = r.data

        if data != i:
            raise

if __name__ == '__main__':
    # stable version
    # client = RiakClient(host='219.224.135.60', port=10018)

    # master_version
    client = RiakClient(host='219.224.135.60', pb_port=10017, protocol='pbc')

    # master_version use nodes param
    # 此场景下性能差不多，和上面指定具体port相比
    # client = RiakClient(host='219.224.135.60', nodes=Nodes, protocol='pbc')

    # test
    test_bucket = client.bucket('lijun_test')
    """
    new_node = test_bucket.new('hehe', data='hehe')
    new_node.store()
    r = test_bucket.get('hehe')
    print r.get_data(), type(r.get_data())
    new_node = test_bucket.new('hehe1', data=1)
    new_node.store()
    r = test_bucket.get('hehe1')
    print r.get_data(), type(r.get_data())
    """

    # test performance
    """
    n = 100000
    test_riak_write(test_bucket, n)
    test_riak_read(test_bucket, n)
    """

    get_results = load_weibos_from_xapian()
    store2riak(get_results)

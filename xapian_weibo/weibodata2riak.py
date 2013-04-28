# -*- coding: utf-8 -*-

from riak.client import RiakClient
from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import load_emotion_words
import riak
import datetime
import time
import opencc
import re

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
    today = datetime.datetime.today()
    end_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    begin_ts = end_ts - 39 * 24 * 3600

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


if __name__ == '__main__':
    # master_version
    client = RiakClient(host='219.224.135.60', pb_port=10017, protocol='pbc')

    get_results = load_weibos_from_xapian()
    store2riak(get_results)

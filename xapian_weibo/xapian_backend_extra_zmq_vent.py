# -*- coding: utf-8 -*-

from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_FLUSH_DB_SIZE, XAPIAN_ZMQ_VENT_PORT
from xapian_backend import Schema, XapianSearch
from utils import timeit
import datetime
import os
import leveldb
import time
import zmq

XAPIAN_FLUSH_DB_SIZE = XAPIAN_FLUSH_DB_SIZE * 10
LEVELDBPATH = '/home/arthas/leveldb'
SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)


@timeit
def _load_weibos_from_xapian():
    begin_ts = time.mktime(datetime.datetime(2013, 1, 1).timetuple())
    end_ts = time.mktime(datetime.datetime(2013, 1, 3).timetuple())

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }

    s = XapianSearch(path='/home/arthas/dev/xapian_weibo/data', name='master_timeline_weibo')
    count, get_results = s.search(query=query_dict, fields=['_id', 'user', 'text', 'timestamp', 'reposts_count'])
    print count
    return get_results

@timeit
def _load_users_from_xapian():
    s = XapianSearch(path='/home/arthas/dev/xapian_weibo/data', name='master_timeline_user')
    results = s.iter_all_docs(fields=['name', 'location', 'province', '_id', 'created_at', 'followers_count', 'statuses_count', 'friends_count', 'bi_followers_count'])
    return results


if __name__ == '__main__':
    """
    then run 'py xapian_backend_extra_zmq_vent.py'
    """
    if SCHEMA_VERSION == 3:
        context = zmq.Context()

        # Socket to send messages on
        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

        count = 0
        ts = time.time()
        tb = ts

        weibo_positive_negative_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_positive_negative_sentiment'),
                                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

        get_results = _load_weibos_from_xapian()

        for item in get_results():
            try:
                sentiment = weibo_positive_negative_sentiment_bucket.Get(str(item['_id']))
            except KeyError:
                sentiment = 0
            item['sentiment'] = int(sentiment)

            sender.send_json(item)
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                print 'deliver cost: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE)
                if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
                    print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
                ts = te

        print 'sleep to give zmq time to deliver '
        print 'until now cost %s sec' % (time.time() - tb)
        time.sleep(10)

    elif SCHEMA_VERSION == 4:
        context = zmq.Context()

        # Socket to send messages on
        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

        count = 0
        ts = time.time()
        tb = ts

        user_domain_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_user_field'),
                                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

        results = _load_users_from_xapian()

        for item in results:
            try:
                domain = user_domain_bucket.Get(str(item['_id']))
            except KeyError:
                domain = 0
            item['domain'] = str(domain)

            sender.send_json(item)
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                print 'deliver cost: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE)
                if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
                    print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
                ts = te

        print 'sleep to give zmq time to deliver '
        print 'until now cost %s sec' % (time.time() - tb)
        time.sleep(10)

    elif SCHEMA_VERSION == 5:
        context = zmq.Context()

        # Socket to send messages on
        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

        count = 0
        ts = time.time()
        tb = ts

        weibo_reposted_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost_20131004'),
                                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

        get_results = _load_weibos_from_xapian()

        for item in get_results():
            try:
                #获得被转发的微博id和uid字符串
                _id_uid_reposted = weibo_reposted_bucket.Get(str(item['_id']))
                #print _id_uid_reposted
            except KeyError:
                _id_uid_reposted = '0_0'
            li = str(_id_uid_reposted).split('_')
            item['_id_reposted'] = int(li[0])
            item['_uid_reposted'] = int(li[1])

            sender.send_json(item)
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                print 'deliver cost: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE)
                if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
                    print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
                ts = te

        print 'sleep to give zmq time to deliver '
        print 'until now cost %s sec' % (time.time() - tb)
        time.sleep(10)
    
    else:
        pass

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


if __name__ == '__main__':
    """
    then run 'py xapian_backend_extra_zmq_vent.py'
    """

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

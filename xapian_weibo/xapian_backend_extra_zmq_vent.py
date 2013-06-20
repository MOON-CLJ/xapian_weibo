# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from xapian_backend_zmq_work import SCHEMA_VERSION, PROCESS_IDX_SIZE
from xapian_backend import Schema, XapianSearch
from utils import timeit
import datetime
import os
import leveldb
import sys
import time
import zmq

PROCESS_IDX_SIZE = PROCESS_IDX_SIZE * 10
LEVELDBPATH = '/home/mirage/leveldb'
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)


@timeit
def _load_weibos_from_xapian():
    begin_ts = time.mktime(datetime.datetime(2012, 9, 1).timetuple())
    end_ts = time.mktime(datetime.datetime(2013, 1, 1).timetuple())

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }

    s = XapianSearch(path='/opt/xapian_weibo/data/20130616/', name='master_timeline_weibo')
    count, get_results = s.search(query=query_dict, fields=['_id', 'user', 'text', 'timestamp'])
    print count
    return get_results


if __name__ == '__main__':
    """
    then run 'py xapian_backend_extra_zmq_vent.py'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    parser = ArgumentParser()
    parser.add_argument('-b', '--bson', action='store_true', help='from bson')
    args = parser.parse_args(sys.argv[1:])

    count = 0
    ts = time.time()
    tb = ts

    weibo_positive_negative_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_positive_negative_sentiment'),
                                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    get_results = _load_weibos_from_xapian()
    for item in get_results():
        sentiment = weibo_positive_negative_sentiment_bucket.Get(str(item['_id']))
        item['sentiment'] = int(sentiment)

        sender.send_json(item)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            print 'deliver cost: %s sec/per %s' % (te - ts, PROCESS_IDX_SIZE)
            if count % (PROCESS_IDX_SIZE * 10) == 0:
                print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
            ts = te

    print 'sleep to give zmq time to deliver '
    print 'until now cost %s sec' % (time.time() - tb)
    time.sleep(10)

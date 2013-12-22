# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_ZMQ_VENT_HOST, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT, XAPIAN_DB_PATH, \
    XAPIAN_ZMQ_PROXY_BACKEND_PORT, XAPIAN_ZMQ_WORK_KILL_INTERVAL, \
    REDIS_HOST, REDIS_PORT
from utils import ts_div_fifteen_m, get_now_db_no

import zmq
import time
import redis
import cPickle as pickle
import zlib


SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
TOP_WEIBOS_REPOSTS_COUNT_LIMIT = 1000
GLOBAL_SENTIMENT_COUNT = "global:%s"  # sentiment,
KEYWORD_SENTIMENT_COUNT = "keyword:%s:%s"  # keyword, sentiment,
TOP_WEIBO_REPOSTS_COUNT_RANK = "top_weibo_rank:%s"  # sentiment,
TOP_WEIBO_KEY = 'top_weibo:%s'  # id,
TOP_KEYWORDS_RANK = 'top_keywords:%s'  # sentiment,


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_keywords():
    "todo"
    return ['现在']


if __name__ == '__main__':
    """
    cd data/
    py ../xapian_weibo/xapian_backend_zmq_work.py -r
    """
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_PROXY_BACKEND_PORT))

    if SCHEMA_VERSION in [2, 5]:
        now_db_no = get_now_db_no()
        print "redis db no now", now_db_no
        r = _default_redis(db=now_db_no)
        keywords = get_keywords()
        while 1:
            new_db_no = get_now_db_no()
            if new_db_no != now_db_no:
                now_db_no = new_db_no
                print "redis db no now", now_db_no
                r = _default_redis(db=now_db_no)

            item = receiver.recv_json()

            sentiment = item['sentiment']
            # global sentiment
            r.incr(GLOBAL_SENTIMENT_COUNT % sentiment)

            # keyword sentiment
            terms = set([term.encode('utf-8') for term in item['terms']])
            for w in keywords:
                if w in terms:
                    r.incr(KEYWORD_SENTIMENT_COUNT % (w, sentiment))

            # top weibos
            reposts_count = item['reposts_count']
            if reposts_count > TOP_WEIBOS_REPOSTS_COUNT_LIMIT:
                r.zadd(TOP_WEIBO_REPOSTS_COUNT_RANK % sentiment, reposts_count, item['_id'])
                r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

                # top keywords
                for t in terms:
                    r.zincrby(TOP_KEYWORDS_RANK % sentiment, t, 1.0)
    else:
        while 1:
            item = receiver.recv_json()

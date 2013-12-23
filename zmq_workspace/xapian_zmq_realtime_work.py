# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_ZMQ_VENT_HOST, \
    XAPIAN_ZMQ_PROXY_BACKEND_PORT, \
    REDIS_HOST, REDIS_PORT
from utils import get_now_db_no, single_word_whitelist

import zmq
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
DOMAIN_SENTIMENT_COUNT = "domain:%s:%s"  # domain, sentiment,
DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK = "domain:%s:top_weibo_rank:%s"  # domain, sentiment,
DOMAIN_TOP_KEYWORDS_RANK = 'domain:%s:top_keywords:%s'  # domain, sentiment,
SENTIMENT_TOPIC_KEYWORDS = "topics:sentiment"
DOMAIN_USERS = "domain_users:%s" # domain


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_keywords():
    r = _default_redis()
    keywords_set = r.smembers(SENTIMENT_TOPIC_KEYWORDS)
    return list(keywords_set)


def get_domain_users():
    r = _default_redis()
    domain_users = {}
    for i in range(9):
        domain_user_set = r.smembers(DOMAIN_USERS % i)
        domain_users[i] = domain_user_set

    return domain_users


def realtime_sentiment_cal(item):
    sentiment = item['sentiment']
    # global sentiment
    r.incr(GLOBAL_SENTIMENT_COUNT % sentiment)

    # keyword sentiment
    terms = [term.encode('utf-8') for term in item['terms']]
    terms = set(filter(lambda x: x not in single_word_whitelist, terms))

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

    # -- domain --
    for domain, d_users in domain_users.iteritems():
        if item['user'] in d_users:
            # sentiment
            r.incr(DOMAIN_SENTIMENT_COUNT % (domain, sentiment))

            # top weibos
            if reposts_count > TOP_WEIBOS_REPOSTS_COUNT_LIMIT:
                r.zadd(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (domain, sentiment), reposts_count, item['_id'])
                # 不用再存微博了，上面已经存过一次了

                # top keywords
                for t in terms:
                    r.zincrby(DOMAIN_TOP_KEYWORDS_RANK % (domain, sentiment), t, 1.0)


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
        # prepare
        now_db_no = get_now_db_no()
        print "redis db no now", now_db_no
        r = _default_redis(db=now_db_no)
        keywords = get_keywords()
        domain_users = get_domain_users()

        while 1:
            new_db_no = get_now_db_no()
            if new_db_no != now_db_no:
                now_db_no = new_db_no
                print "redis db no now", now_db_no
                r = _default_redis(db=now_db_no)

            item = receiver.recv_json()
            realtime_sentiment_cal(item)
    else:
        while 1:
            item = receiver.recv_json()

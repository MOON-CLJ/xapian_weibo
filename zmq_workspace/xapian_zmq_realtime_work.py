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
import datetime


SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
TOP_WEIBOS_REPOSTS_COUNT_LIMIT = 1000
GLOBAL_SENTIMENT_COUNT = "global:%s"  # sentiment,
TOP_WEIBO_REPOSTS_COUNT_RANK = "top_weibo_rank:%s"  # sentiment,
TOP_WEIBO_KEY = 'top_weibo:%s'  # id,
TOP_KEYWORDS_RANK = 'top_keywords:%s'  # sentiment,
KEYWORD_SENTIMENT_COUNT = "keyword:%s:%s"  # keyword, sentiment,
KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK = "keyword:%s:top_weibo_rank:%s"  # keyword, sentiment,
KEYWORD_TOP_KEYWORDS_RANK = 'keyword:%s:top_keywords:%s'  # keyword, sentiment,
DOMAIN_SENTIMENT_COUNT = "domain:%s:%s"  # domain, sentiment,
DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK = "domain:%s:top_weibo_rank:%s"  # domain, sentiment,
DOMAIN_TOP_KEYWORDS_RANK = 'domain:%s:top_keywords:%s'  # domain, sentiment,
SENTIMENT_TOPIC_KEYWORDS = "sentiment_topic_keywords"
USER_DOMAIN = "user_domain"  # user domain hash,
NOW_DB_START_TS = "now_db_start_ts"  # start ts

# profile_keywords_cal
USER_KEYWORDS = "user_keywords_%s"  # user keywords sorted set, uid,
USER_SET = "user_profile"  # user set,


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_keywords():
    r0 = _default_redis()
    keywords_set = r0.smembers(SENTIMENT_TOPIC_KEYWORDS)
    return keywords_set


def set_now_accepted_tsrange(ts):
    start_ts, end_ts = get_now_tsrange(ts)
    a_start_ts = global_r0.get(NOW_DB_START_TS)
    if not a_start_ts or int(a_start_ts) < start_ts:
        global_r0.set(NOW_DB_START_TS, start_ts)
        print 'set accepted ts range: ', get_now_accepted_tsrange()


def get_now_accepted_tsrange():
    # start_ts: timestamp of 15:00, end_ts: timestamp of 15: 15
    a_start_ts = global_r0.get(NOW_DB_START_TS)
    start_ts, end_ts = get_now_tsrange(a_start_ts)
    return start_ts - 15 * 60, end_ts + 15 * 60


def get_now_tsrange(ts):
    ts = int(ts)
    start_ts = ts - ts % (15 * 60)
    return start_ts, start_ts + 15 * 60


def user2domain(uid):
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1  # not taged label

    return int(domainid)


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


def realtime_sentiment_cal(item):
    sentiment = item['sentiment']
    # global sentiment
    global_r.incr(GLOBAL_SENTIMENT_COUNT % sentiment)

    terms = [term.encode('utf-8') for term in item['terms']]
    terms = filter(lambda x: x not in single_word_whitelist, terms)
    reposts_count = item['reposts_count']

    if reposts_count > TOP_WEIBOS_REPOSTS_COUNT_LIMIT:
        # top weibos
        global_r.zadd(TOP_WEIBO_REPOSTS_COUNT_RANK % sentiment, reposts_count, item['_id'])
        global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        for t in terms:
            # top keywords
            global_r.zincrby(TOP_KEYWORDS_RANK % sentiment, t, 1.0)

    flag_set = set()
    for t in terms:
        if t in global_keywords:
            # keyword sentiment
            global_r.incr(KEYWORD_SENTIMENT_COUNT % (t, sentiment))

            if t not in flag_set:
                # keyword top weibos
                global_r.zadd(KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK % (t, sentiment), reposts_count, item['_id'])
                global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

                for tt in terms:
                    # keyword top keywords
                    global_r.zincrby(KEYWORD_TOP_KEYWORDS_RANK % (t, sentiment), tt, 1.0)
                flag_set.add(t)

    domain = user2domain(item['user'])
    if domain != -1 and domain != 20:
        # domain sentiment
        global_r.incr(DOMAIN_SENTIMENT_COUNT % (domain, sentiment))

        # domain top weibos
        global_r.zadd(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (domain, sentiment), reposts_count, item['_id'])
        global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        for t in terms:
            # domain top keywords
            global_r.zincrby(DOMAIN_TOP_KEYWORDS_RANK % (domain, sentiment), t, 1.0)


def realtime_profile_keywords_cal(item):
    terms_cx = item['terms_cx']
    uid = item['user']
    for term, cx in terms_cx:
        if cx == 'n':
            global_r.zincrby(USER_KEYWORDS % uid, term, 1.0)
            global_r.sadd(USER_SET, uid)


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
        item = receiver.recv_json()
        item_timestamp = item['timestamp']

        now_db_no = get_now_db_no(item_timestamp)
        print "redis db no now", now_db_no
        global_r = _default_redis(db=now_db_no)
        global_r0 = _default_redis()
        set_now_accepted_tsrange(item_timestamp)
        global_keywords = get_keywords()

        while 1:
            item = receiver.recv_json()
            item_timestamp = item['timestamp']

            now_a_start_ts, now_a_end_ts = get_now_accepted_tsrange()
            if int(item_timestamp) < now_a_start_ts or int(item_timestamp) >= now_a_end_ts:
                # 超出接受范围，抛弃该条微博
                continue

            new_db_no = get_now_db_no(item_timestamp)
            if new_db_no != now_db_no:
                now_db_no = new_db_no
                print "redis db no now", now_db_no
                global_r = _default_redis(db=now_db_no)
                set_now_accepted_tsrange(item_timestamp)
                global_keywords = get_keywords()

            realtime_sentiment_cal(item)
            realtime_profile_keywords_cal(item)
    else:
        while 1:
            item = receiver.recv_json()

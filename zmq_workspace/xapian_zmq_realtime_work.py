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
import time
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

# realtime_identify_work
USER_DOMAIN = "user_domain" # user domain hash,
USER_NAME_UID = "user_name_uid" # user name-uid hash
USER_COUNT = "user_count:%s:%s" # date as '20131227', uid


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_keywords():
    r0 = _default_redis()
    keywords_set = r0.smembers(SENTIMENT_TOPIC_KEYWORDS)
    return keywords_set


def user2domain(uid):
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


def username2uid(name):
    uid = global_r0.hget(USER_NAME_UID, str(name))
    if not uid:
        return None

    return int(uid)


def get_now_datestr():
    now_ts = time.time()
    datestr = datetime.date.fromtimestamp(now_ts).isoformat().replace('-', '') # 20131227
    return datestr


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
    # domain sentiment
    global_r.incr(DOMAIN_SENTIMENT_COUNT % (domain, sentiment))

    # domain top weibos
    global_r.zadd(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (domain, sentiment), reposts_count, item['_id'])
    global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

    for t in terms:
        # domain top keywords
        global_r.zincrby(DOMAIN_TOP_KEYWORDS_RANK % (domain, sentiment), t, 1.0)


def realtime_identify_cal(item):
    now_datestr = get_now_datestr()
    uid = item['user']
    domainid = user2domain(uid)
    reposts_count = item['reposts_count']
    comments_count = item['comments_count']
    attitudes_count = 0
    # attitudes_count = item['attitudes_count'] # 此字段缺失
    important = reposts_count + comments_count + attitudes_count

    # 更新该条微博发布用户的重要度、领域
    global_r0.hset(USER_COUNT % (now_datestr, uid), 'domain', domainid)

    global_r0.hincrby(USER_COUNT % (now_datestr, uid), 'active')

    global_r0.hincrby(USER_COUNT % (now_datestr, uid), 'important', important)
    
    # 更新直接转发或原创用户的重要度 + 1
    retweeted_uid = item['retweeted_uid']
    if retweeted_uid != 0:
        # 该条微博为转发微博
        text = item['text']
        repost_users = re.findall('//@([a-zA-Z-_\u0391-\uFFE5]+)', text)

        if len(repost_users):
            direct_username = repost_users[0]
            direct_uid = username2uid(direct_username)

            if direct_uid:
                retweeted_uid = direct_uid

        global_r0.hset(USER_COUNT % (now_datestr, retweeted_uid), 'domain', user2domain(retweeted_uid))

        global_r0.hincrby(USER_COUNT % (now_datestr, retweeted_uid), 'important')


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
        global_keywords = get_keywords()
        now_db_no = get_now_db_no()
        print "redis db no now", now_db_no
        global_r = _default_redis(db=now_db_no)
        global_r0 = _default_redis()

        while 1:
            new_db_no = get_now_db_no()
            if new_db_no != now_db_no:
                global_keywords = get_keywords()
                now_db_no = new_db_no
                print "redis db no now", now_db_no
                global_r = _default_redis(db=now_db_no)

            item = receiver.recv_json()
            realtime_sentiment_cal(item)
            realtime_identify_cal(item)
    else:
        while 1:
            item = receiver.recv_json()

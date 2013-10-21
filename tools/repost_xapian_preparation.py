# -*- coding: utf-8 -*-

import datetime
import time
import re
import os
import leveldb

from xapian_weibo.xapian_backend import XapianSearch

LEVELDBPATH = '/home/mirage/leveldb'

weibo_repost_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_repost_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost_20131004'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)


def getStatusById(mid):
    r = statuses_search.search_by_id(int(mid), fields=['_id', 'user'])
    if r:
        mid = r['_id']
        uid = r['user']
        if not mid:
            return
        return [mid, uid]


def getStatusByNameAndRid(name, rid, r_users):
    count, get_results = user_search.search(query={'name': name}, fields=['_id'])
    uid = None
    if count:
        for r in get_results():
            uid = r['_id']
            break
    else:
        return

    mid = None
    ts = None
    count, get_results = statuses_search.search(query={'user': int(uid), 'retweeted_status': int(rid)}, fields=['_id', 'text', 'timestamp'])
    if count:
        for r in get_results():
            mid = r['_id']
            text = r['text']
            ts = r['timestamp']
            repost_users = re.findall(u'/@([a-zA-Z-_\u0391-\uFFE5]+)', text)
            if repost_users == r_users:
                break

    if not mid or not ts:
        return

    return [mid, uid, ts]


def prepare_from_xapian():
    finish_set = set()  # 存储已经解析上层转发结构的微博

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - 260 * during
    now_ts = now_ts - 180 * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }

    fields_list = ['text', 'timestamp', 'user', '_id', 'retweeted_status']

    count, get_results = statuses_search.search(query=query_dict, fields=fields_list)
    print 'statuses_count: ', count

    process_count = 0
    for r in get_results():
        if process_count % 10000 == 0:
            print process_count
        process_count += 1

        mid = r['_id']
        uid = r['user']
        ts = int(r['timestamp'])
        if mid and uid and ts:
            reposts_ = [mid, uid, ts]
            if mid not in finish_set:
                finish_set.add(mid)
            else:
                continue

        text = r['text']
        repost_users = re.findall(u'/@([a-zA-Z-_\u0391-\uFFE5]+)', text)
        retweeted_status = r['retweeted_status']
        retweeted_mid = None
        retweeted_uid = None
        if retweeted_status:
            if repost_users and repost_users:
                for idx in range(0, len(repost_users)):
                    tp = getStatusByNameAndRid(repost_users[idx], retweeted_status, repost_users[-len(repost_users) + idx + 1: -1])
                    if tp:
                        repost_mid, repost_uid, repost_ts = tp
                        if repost_mid not in finish_set:
                            finish_set.add(repost_mid)
                        else:
                            continue
                        k = str(reposts_[0])
                        v = str(repost_mid) + '_' + str(repost_uid)
                        weibo_repost_bucket.Put(k, v)
                        reposts_ = [repost_mid, repost_uid, repost_ts]
            tp1 = getStatusById(retweeted_status)
            if tp1:
                retweeted_mid, retweeted_uid = tp1
                k = str(reposts_[0])
                v = str(retweeted_mid) + '_' + str(retweeted_uid)
                weibo_repost_bucket.Put(k, v)


if __name__ == '__main__':
    prepare_from_xapian()

#!/usr/bin/env python
#-*-coding:utf-8-*-
'''用户领域分类模型
'''

import os

from xapian_weibo.xapian_backend import XapianSearch

from operator import itemgetter
import datetime
import time
import leveldb


LEVELDBPATH = '/home/mirage/leveldb'
global_user_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_user_field_20131012'),
                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')  # search by index
xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)  # search by index
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']


def readProtoUser():
    protou = {}
    with open("/home/mirage/linhao/project_bishe/weibo/profile/user_classify/protou.txt") as f:
        for line in f:
            area = line.split(":")[0]
            if area not in protou:
                protou[area] = set()
            for u in line.split(":")[1].split():
                protou[area].add(int(u))
    return protou


def readProtoWord():
    protow = {}
    with open("/home/mirage/linhao/project_bishe/weibo/profile/user_classify/protow.txt") as f:
        for line in f:
            area = line.split(":")[0]
            if area not in protow:
                protow[area] = set(line.split(":")[1].split(","))
    return protow


class UserField():
    def __init__(self):
        # get class proto users
        self.protou = readProtoUser()
        # get class proto words
        self.protow = readProtoWord()
        # number of users without friends in proto users
        self.withoutp = 0
        # number of users without statuses
        self.withoutstatus = 0
        b = datetime.datetime(2012, 10, 1)
        self.tb = time.mktime(b.timetuple())
        e = datetime.datetime(2013, 5, 1)
        self.te = time.mktime(e.timetuple())

    def getField(self, uid, friends):
        mbr = {f: 0 for f in fields_value}

        # to record user with friends in proto users
        flag = 0
        for f in friends:
            for area in fields_value:
                if f in self.protou[area]:
                    flag += 1
                    mbr[area] += 1

        # for users no none friends in proto users, get their keywords
        if flag == 0:
            self.withoutp += 1
            # search keywords
            count, get_results = xapian_search_weibo.search(
                query={
                    'user': uid,
                    'timestamp': {'$gt': self.tb, '$lt': self.te}
                },
                sort_by=['timestamp'],
                fields=['terms', 'retweeted_status'])  # search for seed users' words
            if count == 0:
                self.withoutstatus += 1
            for r in get_results():
                for t in r['terms'].iterkeys():
                    for area in fields_value:
                        if t in self.protow[area]:
                            mbr[area] += r['terms'][t]

                # get originated tweets
                if r['retweeted_status'] is not None:
                    rr = xapian_search_weibo.search_by_id(r['retweeted_status'], fields=['terms'])
                    if not rr:
                        continue
                    for t1 in rr['terms'].iterkeys():
                        for area in fields_value:
                            if t1 in self.protow[area]:
                                mbr[area] += rr['terms'][t1]

        sorted_mbr = sorted(mbr.iteritems(), key=itemgetter(1), reverse=True)
        field1 = sorted_mbr[0][0]
        field2 = sorted_mbr[1][0]
        k = str(uid)
        v = ','.join([str(fields_value.index(field1)), str(fields_value.index(field2))])
        global_user_field_bucket.Put(k, v)

    def getWithoutTp(self):
        print 'count of users without friends in proto users: ', self.withoutp

    def getWithoutStatus(self):
        print 'count of users without statuses: ', self.withoutstatus


if __name__ == '__main__':
    uf = UserField()
    user_count = 0
    ts = te = time.time()
    iter_users = xapian_search_user.iter_all_docs(fields=['_id', 'friends'])
    for user in iter_users:
        uf.getField(user['_id'], user['friends'])
        user_count += 1
        if user_count % 10000 == 0:
            te = time.time()
            print user_count, '%s sec' % (te - ts)
            ts = te

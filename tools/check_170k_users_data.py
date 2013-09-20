# -*- coding:utf-8 -*-

import sys

sys.path.append('../xapian_weibo')
from xapian_backend import XapianSearch
from utils4scrapy.tk_maintain import _default_mongo

# 默认schema_version为2
s = XapianSearch(path='../data/', name='master_timeline_weibo')
mongo = _default_mongo(host='219.224.135.60', usedb='master_timeline')

existed_file = open('2011_emotion_users_existed_20130615.txt', 'w')
missing_file = open('2011_emotion_users_missing_20130615.txt', 'w')
with open('/home/arthas/dev/scrapy_weibo/test/2011_emotion_users.txt') as f:
    missing = 0
    not_exist = 0
    per_page_missing = 30
    iter_count = 0
    for line in f:
        iter_count += 1
        if iter_count % 100 == 0:
            print iter_count, missing, not_exist
        uid = line.split()[0]
        uid = int(uid)
        count = s.search(query={'user': uid}, count_only=True)
        r = mongo.master_timeline_user.find_one({'_id': uid})
        if r:
            page = r['statuses_count'] / 100
            if r['statuses_count'] % 100 > 0:
                page += 1

            if r['statuses_count'] - count > page * per_page_missing and count > 0:
                missing += 1
                missing_file.write('%s\n' % uid)
            elif r['statuses_count'] - count <= page * per_page_missing:
                existed_file.write('%s\n' % uid)
            if count == 0:
                not_exist += 1
                missing_file.write('%s\n' % uid)
        else:
            missing_file.write('%s\n' % uid)
            not_exist += 1

    print missing, not_exist

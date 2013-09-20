# -*- coding: utf-8 -*-

import datetime

from utils4scrapy.tk_maintain import _default_mongo

db = _default_mongo(usedb='master_timeline')

count = 0
for weibo in db.master_timeline_weibo.find():
    if 'user' not in weibo:
        print '.'
        count += 1
        print 'del', weibo['_id'], datetime.date.fromtimestamp(weibo['first_in']), count
        db.master_timeline_weibo.remove({'_id': weibo['_id']})

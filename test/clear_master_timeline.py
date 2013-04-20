# -*- coding: utf-8 -*-

import datetime

from utils4scrapy.tk_maintain import _default_mongo

db = _default_mongo(usedb='master_timeline')

for weibo in db.master_timeline_weibo.find():
    if 'user' not in weibo:
        print '.'
        print 'del', weibo['_id'], datetime.date.fromtimestamp(weibo['first_in'])
        db.master_timeline_weibo.remove({'_id': weibo['_id']})

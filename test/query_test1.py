# -*- coding:utf-8 -*-

import  calendar
import datetime

from xapian_weibo.xapian_backend import XapianSearch

s = XapianSearch(path='/opt/xapian_weibo/data/', name='statuses', schema_version=1)

begin_ts1 = calendar.timegm(datetime.datetime(2011, 1, 1).timetuple())
end_ts1 = calendar.timegm(datetime.datetime(2011, 12, 31).timetuple())


query_dict = {
    'ts': {
        '$gt': begin_ts1,
        '$lt': end_ts1,
    }
}
count, get_results = s.search(query=query_dict, start_offset=500000, max_offset=500000,fields=['_id','repost'])

print count

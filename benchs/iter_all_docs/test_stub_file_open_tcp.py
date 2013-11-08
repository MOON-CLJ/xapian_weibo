# -*- coding:utf-8 -*-

import sys
import datetime

sys.path.append('../../xapian_weibo')
from xapian_backend import XapianSearch

stub = 'master_timeline_weibo_stub'
s = XapianSearch(stub=stub, include_remote=True)
results = s.iter_all_docs()
for r in results:
    pass

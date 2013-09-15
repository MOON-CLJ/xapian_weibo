# -*- coding: utf-8 -*-

import redis
r = redis.StrictRedis()
"""
print len(r.keys('linhao_friends*'))
print len(r.keys('linhao_followers*'))
for k in r.keys('linhao_friends*'):
    r.delete(k)

for k in r.keys('linhao_followers*'):
    r.delete(k)
"""

print r.keys()

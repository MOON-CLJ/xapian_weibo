# -*- coding: utf-8 -*-

import time
# import msgpack
import simplejson as json
from pyelevator import WriteBatch, Elevator
from utils4scrapy.tk_maintain import _default_mongo


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


@timeit
def load_weibos_from_mongo(limit):
    weibos = []
    for weibo in mongo.master_timeline_weibo.find().limit(limit):
        weibos.append(weibo)

    print 'load', len(weibos), 'weibos'
    return weibos


@timeit
def elevator_multi_write(weibos):
    with WriteBatch('testdb', timeout=1000) as batch:
        for weibo in weibos:
            # batch.Put(str(weibo['id']), msgpack.packb(weibo))
            batch.Put(str(weibo['id']), json.dumps(weibo))


@timeit
def elevator_multi_read(weibo_ids):
    weibos = db.MGet(weibo_ids)
    # weibos = [msgpack.unpackb(weibo) if weibo else None for weibo in weibos]
    weibos = [json.loads(weibo) if weibo else None for weibo in weibos]
    return weibos


def test_rw(n):
    weibos_from_mongo = load_weibos_from_mongo(n)
    elevator_multi_write(weibos_from_mongo)
    weibo_ids = [str(weibo['id']) for weibo in weibos_from_mongo]
    weibos_from_elevator = elevator_multi_read(weibo_ids)

    for i in xrange(len(weibos_from_mongo)):
        if weibos_from_mongo[i] != weibos_from_elevator[i]:
            print '** ' * 10, i

if __name__ == '__main__':
    mongo = _default_mongo(usedb='master_timeline')
    db = Elevator(timeout=1000)
    db.createdb('testdb')
    db.connect('testdb')
    test_rw(10000)
    db.dropdb('testdb')

    """
    load 100000 weibos
    'load_weibos_from_mongo' args: 7.71 sec
    'elevator_multi_read' args: 14.73 sec
    结论是elevator并不足以投入prod使用
    """

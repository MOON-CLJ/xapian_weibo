# -*- coding: utf-8 -*-

import hurdles
import simplejson as json
import msgpack
from utils4scrapy.tk_maintain import _default_mongo


class BenchSimplejsonMsgpack(hurdles.BenchCase):
    def setUp(self):
        n = 100000
        self.weibos_to_encode = self._load_weibos_from_mongo(n)
        self.weibos_to_decode_json = [json.dumps(weibo) for weibo in self.weibos_to_encode]
        self.weibos_to_decode_msgpack = [msgpack.packb(weibo) for weibo in self.weibos_to_encode]

    def tearDown(self):
        pass

    def _load_weibos_from_mongo(self, limit):
        weibos = []
        mongo = _default_mongo(usedb='master_timeline')
        for weibo in mongo.master_timeline_weibo.find().limit(limit):
            weibos.append(weibo)

        print 'load', len(weibos), 'weibos'
        return weibos

    def bench_simplejson_dumps(self):
        for weibo in self.weibos_to_encode:
            json.dumps(weibo)

    def bench_msgpack_packb(self):
        for weibo in self.weibos_to_encode:
            msgpack.packb(weibo)

    def bench_simplejson_loads(self, *args, **kwargs):
        for weibo in self.weibos_to_decode_json:
            json.loads(weibo)

    def bench_msgpack_unpackb(self, *args, **kwargs):
        for weibo in self.weibos_to_decode_msgpack:
            msgpack.unpackb(weibo)

    """
    hurdles bench_simplejson_msgpack.py
    load 100000 weibos
    BenchSimplejsonMsgpack.bench_msgpack_packb
    | average       4804.949 ms
    | median        4805.27 ms
    | fastest       4800.99 ms
    | slowest       4807.43 ms
    load 100000 weibos
    BenchSimplejsonMsgpack.bench_msgpack_unpackb
    | average       1271.494 ms
    | median        1271.38 ms
    | fastest       1269.81 ms
    | slowest       1274.86 ms
    load 100000 weibos
    BenchSimplejsonMsgpack.bench_simplejson_dumps
    | average       6369.791 ms
    | median        6365.03 ms
    | fastest       6353.45 ms
    | slowest       6427.96 ms
    load 100000 weibos
    BenchSimplejsonMsgpack.bench_simplejson_loads
    | average       6969.229 ms
    | median        6943.07 ms
    | fastest       6913.58 ms
    | slowest       7087.94 ms

    ------------------------------------------------------------
    Ran 4 benchmarks
    说明msgpack比simplejson要快，确定往上面转
    """

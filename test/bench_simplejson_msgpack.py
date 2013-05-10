# -*- coding: utf-8 -*-

import hurdles
import simplejson as json
import msgpack
from utils4scrapy.tk_maintain import _default_mongo


class BenchSimplejsonMsgpack(hurdles.BenchCase):
    def setUp(self):
        n = 1000
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

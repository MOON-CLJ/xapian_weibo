# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from utils4scrapy.tk_maintain import _default_mongo
from xapian_backend_zmq_work import SCHEMA_VERSION, PROCESS_IDX_SIZE
from xapian_backend import Schema
import simplejson as json
import sys
import time
import zmq

MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)
db = _default_mongo(MONGOD_HOST, MONGOD_PORT, usedb=schema['db'])
collection = schema['collection']


def load_weibos(db, collection, debug=False):
    if not debug:
        weibos = getattr(db, collection).find(timeout=False)
        print 'prod mode: 从mongodb加载[%s]里的所有微博' % collection
    else:
        with open("../test/sample_tweets.js") as f:
            weibos = json.loads(f.readline())
        print 'debug mode: 从测试数据文件中加载微博'
    return weibos


if __name__ == "__main__":
    """
    then run 'py xapian_backend_zmq_vent.py -d'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    parser = ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='DEBUG')
    args = parser.parse_args(sys.argv[1:])
    debug = args.debug

    count = 0
    ts = time.time()
    for weibo in load_weibos(db, collection, debug):
        sender.send_json(weibo)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            print 'deliver cost: %s sec/per %s' % (te - ts, PROCESS_IDX_SIZE)
            ts = te

    print 'sleep to give zmq time to deliver '
    time.sleep(10)

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


def load_items(db, collection, debug=False):
    if not debug:
        items = getattr(db, collection).find(timeout=False)
        print 'prod mode: 从mongodb加载[%s]里的所有数据' % collection
    else:
        with open("../test/sample_tweets.js") as f:
            items = json.loads(f.readline())
        print 'debug mode: 从测试数据文件中加载数据'
    return items


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
    tb = ts
    for item in load_items(db, collection, debug):
        sender.send_json(item)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            print 'deliver cost: %s sec/per %s' % (te - ts, PROCESS_IDX_SIZE)
            if count % (PROCESS_IDX_SIZE * 100) == 0:
                print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
            ts = te

    print 'sleep to give zmq time to deliver '
    print 'until now cost %s sec' % (time.time() - tb)
    time.sleep(10)

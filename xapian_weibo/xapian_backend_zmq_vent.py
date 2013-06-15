# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from utils4scrapy.tk_maintain import _default_mongo
from xapian_backend_zmq_work import SCHEMA_VERSION, PROCESS_IDX_SIZE
from bs_input import KeyValueBSONInput
from xapian_backend import Schema
import sys
import time
import zmq

MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)
db = _default_mongo(MONGOD_HOST, MONGOD_PORT, usedb=schema['db'])
collection = schema['collection']

BSON_FILEPATH = "/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson"


def load_items_from_mongo(db, collection):
    items = getattr(db, collection).find(timeout=False)
    print 'prod mode: 从mongodb加载[%s]里的所有数据' % collection
    return items


def load_items_from_bson(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


def send_one_item(sender, item, count, tb, ts):
    sender.send_json(item)
    count += 1
    if count % PROCESS_IDX_SIZE == 0:
        te = time.time()
        print 'deliver cost: %s sec/per %s' % (te - ts, PROCESS_IDX_SIZE)
        if count % (PROCESS_IDX_SIZE * 100) == 0:
            print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
        ts = te

if __name__ == '__main__':
    """
    then run 'py xapian_backend_zmq_vent.py -b'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    parser = ArgumentParser()
    parser.add_argument('-b', '--bson', action='store_true', help='from bson')
    args = parser.parse_args(sys.argv[1:])
    from_bson = args.bson

    if from_bson:
        bs_input = load_items_from_bson()

    count = 0
    ts = time.time()
    tb = ts

    if from_bson:
        for _, item in bs_input.reads():
            send_one_item(sender, item, tb, ts)
    else:
        for item in load_items_from_mongo(db, collection):
            send_one_item(sender, item, tb, ts)

    if from_bson:
        bs_input.close()

    print 'sleep to give zmq time to deliver '
    print 'until now cost %s sec' % (time.time() - tb)
    time.sleep(10)

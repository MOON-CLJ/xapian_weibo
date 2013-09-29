# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_ZMQ_VENT_PORT, XAPIAN_FLUSH_DB_SIZE
from bs_input import KeyValueBSONInput
from xapian_backend import Schema
import sys
import time
import zmq

XAPIAN_FLUSH_DB_SIZE = XAPIAN_FLUSH_DB_SIZE * 10
SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)

BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson'


def load_items_from_bson(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


if __name__ == '__main__':
    """
    'py xapian_backend_zmq_vent.py -b'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

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
            sender.send_json(item)
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                print 'deliver cost: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE)
                if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
                    print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
                ts = te

    if from_bson:
        bs_input.close()

    print 'sleep to give zmq time to deliver '
    print 'until now cost %s sec' % (time.time() - tb)
    time.sleep(10)

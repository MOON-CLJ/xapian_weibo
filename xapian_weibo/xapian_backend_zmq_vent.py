# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from consts import XAPIAN_INDEX_SCHEMA_VERSION, \
        XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT, \
        XAPIAN_FLUSH_DB_SIZE, BSON_FILEPATH
from bs_input import KeyValueBSONInput
from xapian_backend import Schema
import sys
import time
import zmq

XAPIAN_FLUSH_DB_SIZE = XAPIAN_FLUSH_DB_SIZE * 10
SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)


def load_items_from_bson(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


def send_all(bs_input, sender):
    count = 0
    tb = time.time()
    ts = tb
    for _, item in bs_input.reads():
        """
        还没等work连上，就开始在发了
        但如果work长时间没连上，zmq的后台发送队列会满，又会阻塞发送
        """
        sender.send_json(item)
        count += 1
        if count % XAPIAN_FLUSH_DB_SIZE == 0:
            te = time.time()
            print 'deliver speed: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE)
            if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
                print 'total deliver %s, cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
            ts = te

    total_cost = time.time() - tb
    return count, total_cost


if __name__ == '__main__':
    """
    'py xapian_backend_zmq_vent.py -b'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

    # Socket for worker control
    controller = context.socket(zmq.PUB)
    controller.bind("tcp://*:%s" % XAPIAN_ZMQ_CTRL_VENT_PORT)

    parser = ArgumentParser()
    parser.add_argument('-b', '--bson', action='store_true', help='from bson')
    args = parser.parse_args(sys.argv[1:])
    from_bson = args.bson

    if from_bson:
        bs_input = load_items_from_bson()

    if from_bson:
        count, total_cost = send_all(bs_input, sender)

    if from_bson:
        bs_input.close()

    # Send kill signal to workers
    controller.send("KILL")
    print 'send "KILL" to workers'

    print 'sleep to give zmq time to deliver'
    print 'total deliver %s, cost %s sec' % (count, total_cost)
    time.sleep(10)

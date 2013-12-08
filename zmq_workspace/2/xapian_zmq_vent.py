# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT
from index_utils import load_items_from_bson, send_all
from xapian_backend import Schema
import time
import zmq

SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION
schema = getattr(Schema, 'v%s' % SCHEMA_VERSION)


if __name__ == '__main__':
    """
    'py xapian_backend_zmq_vent.py'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:%s" % XAPIAN_ZMQ_VENT_PORT)

    # Socket for worker control
    controller = context.socket(zmq.PUB)
    controller.bind("tcp://*:%s" % XAPIAN_ZMQ_CTRL_VENT_PORT)

    from consts import FROM_BSON
    from_bson = FROM_BSON

    load_origin_data_func = None
    if from_bson:
        bs_input = load_items_from_bson()
        load_origin_data_func = bs_input.reads

    count, total_cost = send_all(load_origin_data_func, sender)

    if from_bson:
        bs_input.close()

    # Send kill signal to workers
    controller.send("KILL")
    print 'send "KILL" to workers'

    print 'sleep to give zmq time to deliver'
    print 'total deliver %s, cost %s sec' % (count, total_cost)
    time.sleep(10)

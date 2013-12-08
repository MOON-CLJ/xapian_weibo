# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT
from index_utils import load_items_from_csv, prefunc_send
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

    from consts import FROM_CSV
    from_csv = FROM_CSV
    
    if from_csv:
        from consts import CSV_FILEPATH
        if os.path.isdir(CSV_FILEPATH):
            files = os.listdir(CSV_FILEPATH)
            for f in files:
                csv_input = load_items_from_csv(CSV_FILEPATH + f)
                count, total_cost = prefunc_send(csv_input, sender)
                csv_input.close()
        elif os.path.isfile(CSV_FILEPATH):
            csv_input = load_items_from_csv(CSV_FILEPATH)
            count, total_cost = prefunc_send(csv_input, sender)
            csv_input.close()


    # Send kill signal to workers
    controller.send("KILL")
    print 'send "KILL" to workers'

    print 'sleep to give zmq time to deliver'
    print 'total deliver %s, cost %s sec' % (count, total_cost)
    time.sleep(10)

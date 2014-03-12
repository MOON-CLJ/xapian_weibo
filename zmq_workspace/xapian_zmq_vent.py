# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, FROM_BSON, FROM_CSV, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT
from index_utils import send_all
if FROM_BSON:
    from index_utils import load_items_from_bson
if FROM_CSV:
    from index_utils import load_items_from_csv
from xapian_backend import Schema
from csv2json import itemLine2Dict
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

    from consts import FROM_BSON, FROM_CSV
    from_bson = FROM_BSON
    from_csv = FROM_CSV

    def bs_input_pre_func(item):
        item = item[1]
        return item

    def csv_input_pre_func(item):
        item = itemLine2Dict(item)
        return item

    if from_bson:
        pre_funcs = [bs_input_pre_func]
        bs_input = load_items_from_bson()
        load_origin_data_func = bs_input.reads
        count, total_cost = send_all(load_origin_data_func, sender, pre_funcs=pre_funcs)
        bs_input.close()
    elif from_csv:
        pre_funcs = [csv_input_pre_func]
        from consts import CSV_FILEPATH
        if os.path.isdir(CSV_FILEPATH):
            total_cost = 0
            count = 0
            files = os.listdir(CSV_FILEPATH)
            files = sorted([int(f[:-4]) for f in files])
            for f in files:
                csv_input = load_items_from_csv(os.path.join(CSV_FILEPATH, str(f) + '.csv'))
                load_origin_data_func = csv_input.__iter__
                tmp_count, tmp_cost = send_all(load_origin_data_func, sender, pre_funcs=pre_funcs)
                total_cost += tmp_cost
                count += tmp_count
                csv_input.close()
        elif os.path.isfile(CSV_FILEPATH):
            csv_input = load_items_from_csv(CSV_FILEPATH)
            load_origin_data_func = csv_input.__iter__
            count, total_cost = send_all(load_origin_data_func, sender, pre_funcs=pre_funcs)
            csv_input.close()

    # Send kill signal to workers
    controller.send("KILL")
    print 'send "KILL" to workers'

    print 'sleep to give zmq time to deliver'
    print 'total deliver %s, cost %s sec' % (count, total_cost)
    time.sleep(10)

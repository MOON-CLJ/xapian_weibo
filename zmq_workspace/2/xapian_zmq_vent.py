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
if SCHEMA_VERSION in [3, 4, 5]:
    import os
    import leveldb
    from consts import XAPIAN_EXTRA_FIELD
    from index_utils import fill_field_from_leveldb
    LEVELDBPATH = '/home/arthas/leveldb'
    if SCHEMA_VERSION == 3:
        leveldb_dbname = 'huyue_weibo_positive_negative_sentiment'
    leveldb_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, leveldb_dbname),
                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


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

    if SCHEMA_VERSION in [3, 4, 5]:
        extra_source = {}
        extra_source['bucket'] = leveldb_bucket
        extra_source['extra_field'] = XAPIAN_EXTRA_FIELD
        count, total_cost = send_all(load_origin_data_func, sender, extra_source, fill_field_funcs=[fill_field_from_leveldb])
    else:
        count, total_cost = send_all(load_origin_data_func, sender)

    if from_bson:
        bs_input.close()

    # Send kill signal to workers
    controller.send("KILL")
    print 'send "KILL" to workers'

    print 'sleep to give zmq time to deliver'
    print 'total deliver %s, cost %s sec' % (count, total_cost)
    time.sleep(10)

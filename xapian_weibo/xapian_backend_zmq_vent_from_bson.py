# -*- coding: utf-8 -*-

from xapian_backend_zmq_work import PROCESS_IDX_SIZE
from bs_input import KeyValueBSONInput
import time
import zmq

BSON_FILEPATH = "/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson"


def load_items(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


if __name__ == "__main__":
    """
    then run 'py xapian_backend_zmq_vent_from_bson.py'
    """

    context = zmq.Context()

    # Socket to send messages on
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    count = 0
    ts = time.time()
    tb = ts
    bs_input = load_items()
    for _id, item in bs_input.reads():
        sender.send_json(item)
        count += 1
        if count % PROCESS_IDX_SIZE == 0:
            te = time.time()
            print 'deliver cost: %s sec/per %s' % (te - ts, PROCESS_IDX_SIZE)
            if count % (PROCESS_IDX_SIZE * 100) == 0:
                print 'total deliver %s cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
            ts = te
    bs_input.close()

    print 'sleep to give zmq time to deliver '
    print 'until now cost %s sec' % (time.time() - tb)
    time.sleep(10)

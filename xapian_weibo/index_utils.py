# -*- coding: utf-8 -*-

from consts import BSON_FILEPATH, XAPIAN_FLUSH_DB_SIZE
from bs_input import KeyValueBSONInput
import time

XAPIAN_FLUSH_DB_SIZE = XAPIAN_FLUSH_DB_SIZE * 10


def load_items_from_bson(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中加载微博'
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input


def fill_field_from_leveldb(item, extra_source):
    try:
        value = extra_source.get('bucket').Get(str(item['_id']))
    except KeyError:
        value = 0
    item[extra_source.get('extra_field')] = int(value)


def send_all(load_origin_data_func, sender, extra_source={}, fill_field_funcs=[]):
    count = 0
    tb = time.time()
    ts = tb
    for _, item in load_origin_data_func():
        """
        还没等work连上，就开始在发了
        但如果work长时间没连上，zmq的后台发送队列会满，又会阻塞发送
        """
        for func in fill_field_funcs:
            func(item, extra_source)
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

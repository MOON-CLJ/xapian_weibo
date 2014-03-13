# -*- coding: utf-8 -*-

from consts import FROM_BSON, FROM_CSV, XAPIAN_FLUSH_DB_SIZE, XAPIAN_ZMQ_WORK_KILL_INTERVAL, XAPIAN_ZMQ_POLL_TIMEOUT
from bs_input import KeyValueBSONInput
from datetime import datetime
import time
import zmq
import sys

if FROM_BSON:
    from consts import BSON_FILEPATH

    def load_items_from_bson(bs_filepath=BSON_FILEPATH):
        print 'bson file mode: 从备份的BSON文件中加载数据'
        bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
        return bs_input

if FROM_CSV:
    def load_items_from_csv(csv_filepath):
        print 'csv file mode: 从CSV文件中加载数据'
        csv_input = open(csv_filepath)
        return csv_input


def send_all(load_origin_data_func, sender, pre_funcs=[]):
    count = 0
    tb = time.time()
    ts = tb
    for item in load_origin_data_func():
        """
        还没等work连上，就开始在发了
        但如果work长时间没连上，zmq的后台发送队列会满，又会阻塞发送
        """
        if pre_funcs:
            for func in pre_funcs:
                item = func(item)
        if item is None:
            continue
        sender.send_json(item)
        count += 1
        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print 'deliver speed: %s sec/per %s' % (te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print 'total deliver %s, cost: %s sec [avg: %sper/sec]' % (count, te - tb, count / (te - tb))
            ts = te
    total_cost = time.time() - tb
    return count, total_cost


def index_forever(xapian_indexer, receiver, controller, poller, sender=None, fill_field_funcs=[]):
    """
    Process index forever
    """
    count = 0
    ts = time.time()
    tb = ts
    receive_kill = False
    while 1:
        evts = poller.poll(XAPIAN_ZMQ_POLL_TIMEOUT)
        if evts:
            socks = dict(poller.poll(XAPIAN_ZMQ_POLL_TIMEOUT))
        elif receive_kill and time.time() - tb > XAPIAN_ZMQ_WORK_KILL_INTERVAL:
            """
            定期kill，可以记录work开启的时间
            然后收到kill的时候判断一下当前时间减去work开启的时间
            是否超过某个阈值，是则执行kill操作
            配套的prod模式下，应该在每隔XAPIAN_ZMQ_WORK_KILL_INTERVAL新开work
            """
            xapian_indexer.close()
            print 'receive "KILL", worker stop, finally close db, cost: %ss' % (time.time() - tb)
            break
        else:
            socks = None

        if socks and socks.get(receiver) == zmq.POLLIN:
            item = receiver.recv_json()
            if fill_field_funcs:
                for func in fill_field_funcs:
                    item = func(item)
            xapian_indexer.add_or_update(item)
            if sender:
                try:
                    sender.send_json(item)
                except:
                    print item
            
            count += 1
            if count % XAPIAN_FLUSH_DB_SIZE == 0:
                te = time.time()
                cost = te - ts
                ts = te
                print '[%s] [%s] total indexed: %s, %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), xapian_indexer.db_folder, count, cost, XAPIAN_FLUSH_DB_SIZE)

        # Any waiting controller command acts as 'KILL'
        if socks and socks.get(controller) == zmq.POLLIN:
            controller.recv()
            receive_kill = True


class InvalidSchemaError(Exception):
    """Raised when schema not match."""
    pass

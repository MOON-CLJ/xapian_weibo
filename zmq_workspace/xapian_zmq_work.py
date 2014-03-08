# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_INDEX_SCHEMA_VERSION, XAPIAN_ZMQ_VENT_HOST, \
    XAPIAN_ZMQ_VENT_PORT, XAPIAN_ZMQ_CTRL_VENT_PORT, XAPIAN_DB_PATH, \
    XAPIAN_ZMQ_PROXY_FRONTEND_PORT, REALTIME_WORK_ON
from index_utils import index_forever, InvalidSchemaError
from xapian_index import XapianIndex
from utils import load_scws, cut

from argparse import ArgumentParser
import zmq

SCHEMA_VERSION = XAPIAN_INDEX_SCHEMA_VERSION


if __name__ == '__main__':
    """
    cd data/
    py ../xapian_weibo/xapian_backend_zmq_work.py -r
    """
    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_VENT_PORT))

    sender = None
    if REALTIME_WORK_ON:
        # Socket to send messages on
        sender = context.socket(zmq.PUSH)
        sender.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_PROXY_FRONTEND_PORT))

    # Socket for control input
    controller = context.socket(zmq.SUB)
    controller.connect('tcp://%s:%s' % (XAPIAN_ZMQ_VENT_HOST, XAPIAN_ZMQ_CTRL_VENT_PORT))
    controller.setsockopt(zmq.SUBSCRIBE, "")

    # Process messages from receiver and controller
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(controller, zmq.POLLIN)

    parser = ArgumentParser()
    parser.add_argument('-r', '--remote_stub', action='store_true', help='remote stub')
    args = parser.parse_args(sys.argv[1:])
    remote_stub = args.remote_stub

    dbpath = XAPIAN_DB_PATH
    if SCHEMA_VERSION not in [1, 2, 5]:
        raise InvalidSchemaError()
    xapian_indexer = XapianIndex(dbpath, SCHEMA_VERSION, remote_stub)

    fill_field_funcs = []
    if SCHEMA_VERSION in [2, 5]:
        ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
        sys.path.append(ab_path)
        from consts import XAPIAN_EXTRA_FIELD
        from triple_sentiment_classifier import triple_classifier

        def fill_sentiment(item):
            sentiment = triple_classifier(item)
            item[XAPIAN_EXTRA_FIELD] = sentiment
            return item
        fill_field_funcs.append(fill_sentiment)

        s = load_scws()

        def cut_text(item):
            text = item['text'].encode('utf-8')
            terms_cx = cut(s, text, cx=True)
            terms = [term for term, cx in terms_cx]
            item['terms'] = terms
            item['terms_cx'] = terms_cx
            return item
        fill_field_funcs.append(cut_text)
    index_forever(xapian_indexer, receiver, controller, poller, sender=sender, fill_field_funcs=fill_field_funcs)

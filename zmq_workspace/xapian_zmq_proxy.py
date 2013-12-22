# -*- coding: utf-8 -*-

import sys
import os
ab_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../xapian_weibo')
sys.path.append(ab_path)

from consts import XAPIAN_ZMQ_PROXY_FRONTEND_PORT, XAPIAN_ZMQ_PROXY_BACKEND_PORT

import zmq


def main():
    """ main method """

    context = zmq.Context(1)

    # Socket facing clients
    frontend = context.socket(zmq.PULL)
    frontend.bind("tcp://*:%s" % XAPIAN_ZMQ_PROXY_FRONTEND_PORT)

    # Socket facing services
    backend = context.socket(zmq.PUSH)
    backend.bind("tcp://*:%s" % XAPIAN_ZMQ_PROXY_BACKEND_PORT)

    zmq.device(zmq.QUEUE, frontend, backend)

    # We never get here…
    frontend.close()
    backend.close()
    context.term()

if __name__ == "__main__":
    main()

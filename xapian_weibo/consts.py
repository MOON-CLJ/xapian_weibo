#!/usr/bin/env python
# -*- coding: utf-8 -*-

PROD_VENV = 0
if PROD_VENV:
    XAPIAN_DATA_DIR = '/var/lib/xapian_weibo'
    XAPIAN_STUB_FILE_DIR = '/var/lib/xapian_weibo/stub'
    XAPIAN_ZMQ_VENT_HOST = ''
else:
    XAPIAN_DATA_DIR = '/Users/clj/dev/xapian_weibo/data'
    XAPIAN_STUB_FILE_DIR = '/Users/clj/dev/xapian_weibo/stub'
    XAPIAN_ZMQ_VENT_HOST = 'localhost'

XAPIAN_INDEX_LOCK_FILE = '/tmp/xapian_weibo'
XAPIAN_INDEX_SCHEMA_VERSION = 2
XAPIAN_ZMQ_VENT_PORT = 5557
XAPIAN_FLUSH_DB_SIZE = 20000

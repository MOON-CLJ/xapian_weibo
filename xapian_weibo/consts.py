#!/usr/bin/env python
# -*- coding: utf-8 -*-

XAPIAN_INDEX_LOCK_FILE = '/tmp/xapian_weibo'
XAPIAN_REMOTE_OPEN_TIMEOUT = 100000  # 100s
XAPIAN_INDEX_SCHEMA_VERSION = 1
XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION = 2
XAPIAN_ZMQ_VENT_PORT = 5557

if XAPIAN_INDEX_SCHEMA_VERSION == 2:
    XAPIAN_DB_PATH = 'master_timeline_weibo'
elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
    XAPIAN_DB_PATH = 'master_timeline_user'
elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
    XAPIAN_DB_PATH = 'master_timeline_sentiment'

PROD_VENV = 1
if PROD_VENV:
    XAPIAN_DATA_DIR = '/home/arthas/var/lib/xapian_weibo/data'
    XAPIAN_STUB_FILE_DIR = '/home/arthas/var/lib/xapian_weibo/stub'
    XAPIAN_ZMQ_VENT_HOST = '219.224.135.61'
    XAPIAN_FLUSH_DB_SIZE = 20000
    if XAPIAN_INDEX_SCHEMA_VERSION == 2:
        BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson'
    elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
        BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_user.bson'
else:
    XAPIAN_DATA_DIR = '/home/arthas/dev/xapian_weibo/data'
    XAPIAN_STUB_FILE_DIR = '/home/arthas/dev/xapian_weibo/stub'
    XAPIAN_ZMQ_VENT_HOST = 'localhost'
    XAPIAN_FLUSH_DB_SIZE = 2000
    if XAPIAN_INDEX_SCHEMA_VERSION == 2:
        BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/test/master_timeline_weibo.bson'
    elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
        BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/test/master_timeline_user.bson'

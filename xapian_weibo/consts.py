#!/usr/bin/env python
# -*- coding: utf-8 -*-

XAPIAN_INDEX_LOCK_FILE = '/tmp/xapian_weibo'
XAPIAN_REMOTE_OPEN_TIMEOUT = 100000  # 100s
XAPIAN_INDEX_SCHEMA_VERSION = 2
XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION = 2

if XAPIAN_INDEX_SCHEMA_VERSION == 2:
    XAPIAN_DB_PATH = 'master_timeline_weibo'
    XAPIAN_ZMQ_VENT_PORT = 5557
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5558
elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
    XAPIAN_DB_PATH = 'master_timeline_user'
    XAPIAN_ZMQ_VENT_PORT = 5559
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5560
elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
    XAPIAN_DB_PATH = 'master_timeline_sentiment'
    XAPIAN_ZMQ_VENT_PORT = 5561
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5562
    # extra
    XAPIAN_EXTRA_FIELD = 'sentiment'

PROD_VENV = 0
FROM_BSON = 1
if PROD_VENV:
    XAPIAN_DATA_DIR = '/var/lib/xapian_weibo'
    XAPIAN_STUB_FILE_DIR = '/var/lib/xapian_weibo/stub'
    if XAPIAN_INDEX_SCHEMA_VERSION == 1:
        XAPIAN_DB_FOLDER_PREFIX = '/var/lib/xapian_weibo/20130000'
    XAPIAN_ZMQ_VENT_HOST = '219.224.135.61'  # 分发机器的ip
    XAPIAN_FLUSH_DB_SIZE = 20000
    XAPIAN_ZMQ_WORK_KILL_INTERVAL = 3600  # 1 hour
    if FROM_BSON:
        if XAPIAN_INDEX_SCHEMA_VERSION == 2:
            BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
            BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_user.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
            BSON_FILEPATH = '/home/arthas/mongodumps/20130516/master_timeline/master_timeline_weibo.bson'
else:
    XAPIAN_DATA_DIR = '/home/arthas/dev/xapian_weibo/data'
    XAPIAN_STUB_FILE_DIR = '/home/arthas/dev/xapian_weibo/stub'
    if XAPIAN_INDEX_SCHEMA_VERSION == 1:
        XAPIAN_DB_FOLDER_PREFIX = '/home/arthas/dev/xapian_weibo/data/20130000'
    XAPIAN_ZMQ_VENT_HOST = 'localhost'
    XAPIAN_FLUSH_DB_SIZE = 2000
    XAPIAN_ZMQ_WORK_KILL_INTERVAL = 0  # immediately
    if FROM_BSON:
        if XAPIAN_INDEX_SCHEMA_VERSION == 2:
            BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
            BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_user.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
            BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_weibo.bson'

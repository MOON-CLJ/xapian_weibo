#!/usr/bin/env python
# -*- coding: utf-8 -*-

XAPIAN_INDEX_LOCK_FILE = '/tmp/xapian_weibo'
XAPIAN_REMOTE_OPEN_TIMEOUT = 300000  # 300s
XAPIAN_INDEX_SCHEMA_VERSION = 1
XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION = 2

if XAPIAN_INDEX_SCHEMA_VERSION == 2:
    XAPIAN_DB_PATH = 'master_timeline_weibo'
    XAPIAN_ZMQ_VENT_PORT = 5557
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5558
    # extra
    XAPIAN_EXTRA_FIELD = 'sentiment'
elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
    XAPIAN_DB_PATH = 'master_timeline_user'
    XAPIAN_ZMQ_VENT_PORT = 5559
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5560
#elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
#    XAPIAN_DB_PATH = 'master_timeline_sentiment'
#    XAPIAN_ZMQ_VENT_PORT = 5561
#    XAPIAN_ZMQ_CTRL_VENT_PORT = 5562
#    # extra
#    XAPIAN_EXTRA_FIELD = 'sentiment'
elif XAPIAN_INDEX_SCHEMA_VERSION == 4:
    XAPIAN_DB_PATH = 'master_timeline_domain'
    XAPIAN_ZMQ_VENT_PORT = 5563
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5564
    XAPIAN_EXTRA_FIELD = 'domain'
elif XAPIAN_INDEX_SCHEMA_VERSION == 5:
    XAPIAN_DB_PATH = 'master_timeline_weibo_csv'
    XAPIAN_ZMQ_VENT_PORT = 5565
    XAPIAN_ZMQ_CTRL_VENT_PORT = 5566

PROD_VENV = 0
FROM_BSON = 1
FROM_CSV = 0
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
            BSON_FILEPATH = '/home/arthas/mongodumps/20131008/master_timeline/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
            BSON_FILEPATH = '/home/arthas/mongodumps/20131008/master_timeline/master_timeline_user.bson'
        #elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
        #    BSON_FILEPATH = '/home/arthas/mongodumps/20131008/master_timeline/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 4:
            BSON_FILEPATH = '/home/arthas/mongodumps/20131008/master_timeline/master_timeline_user.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 5:
            CSV_FILEPATH = ''  # unsure
            raise
else:
    XAPIAN_DATA_DIR = '/home/arthas/dev/data'
    XAPIAN_STUB_FILE_DIR = '/home/arthas/dev/data/stub'
    if XAPIAN_INDEX_SCHEMA_VERSION == 1:
        XAPIAN_DB_FOLDER_PREFIX = '/home/arthas/dev/data/20130000'
    XAPIAN_ZMQ_VENT_HOST = 'localhost'
    XAPIAN_FLUSH_DB_SIZE = 20000
    XAPIAN_ZMQ_WORK_KILL_INTERVAL = 0  # immediately
    if FROM_BSON:
        if XAPIAN_INDEX_SCHEMA_VERSION == 2:
            BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 1:
            BSON_FILEPATH = '/home/arthas/dev/original_data/dump60/master_timeline/master_timeline_user.bson'
        #elif XAPIAN_INDEX_SCHEMA_VERSION == 3:
        #    BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_weibo.bson'
        elif XAPIAN_INDEX_SCHEMA_VERSION == 4:
            BSON_FILEPATH = '/home/arthas/dev/xapian_weibo/tests/master_timeline_user.bson'
    if FROM_CSV:
        if XAPIAN_INDEX_SCHEMA_VERSION == 5:
            CSV_FILEPATH = '/home/arthas/dev/original_data/MB_QL_9_10_NODE10.csv'  # 文件夹时末尾需要/

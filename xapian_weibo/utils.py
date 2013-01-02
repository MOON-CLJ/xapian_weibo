#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scws

SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
IGNORE_PUNCTUATION = 1
# dev
"""
CUSTOM_DICT_PATH = '/Users/clj/dev/xapian_weibo/dict/userdic.txt'
EXTRA_STOPWORD_PATH = '/Users/clj/dev/xapian_weibo/dict/stopword.dic'
EXTRA_EMOTIONWORD_PATH = '/Users/clj/dev/xapian_weibo/dict/emotionlist.txt'
"""

# prod
CUSTOM_DICT_PATH = '/opt/xapian_weibo/dict/userdic.txt'
EXTRA_STOPWORD_PATH = '/opt/xapian_weibo/dict/stopword.dic'
EXTRA_EMOTIONWORD_PATH = '/opt/xapian_weibo/dict/emotionlist.txt'


def load_scws():
    s = scws.Scws()
    s.set_charset(SCWS_ENCODING)

    s.set_dict(CHS_DICT_PATH, scws.XDICT_MEM)
    s.add_dict(CHT_DICT_PATH, scws.XDICT_MEM)
    s.add_dict(CUSTOM_DICT_PATH, scws.XDICT_TXT)

    # 把停用词全部拆成单字，再过滤掉单字，以达到去除停用词的目的
    s.add_dict(EXTRA_STOPWORD_PATH, scws.XDICT_TXT)
    # 即基于表情表对表情进行分词，必要的时候在返回结果处或后剔除
    s.add_dict(EXTRA_EMOTIONWORD_PATH, scws.XDICT_TXT)

    s.set_rules(SCWS_RULES)
    s.set_ignore(IGNORE_PUNCTUATION)
    return s


def load_extra_dic():
    emotion_words = [line.strip('\r\n') for line in file(EXTRA_EMOTIONWORD_PATH)]
    return emotion_words

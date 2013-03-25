#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import scws
import time
import itertools
import collections
import multiprocessing
import operator


SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
IGNORE_PUNCTUATION = 1

ABSOLUTE_DICT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dict'))
CUSTOM_DICT_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'userdic.txt')
EXTRA_STOPWORD_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'stopword.dic')
EXTRA_EMOTIONWORD_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'emotionlist.txt')
EXTRA_ONE_WORD_WHITE_LIST_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'one_word_white_list.txt')


class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        num_workers = multiprocessing.cpu_count() * 2
        self.pool = multiprocessing.Pool(num_workers, maxtasksperchild=10000)

    def partition(self, mapped_values):
        """
        >>> s = [('yellow', 1), ('blue', 2), ('yellow', 3), ('blue', 4), ('red', 1)]
        >>> d = defaultdict(list)
        >>> for k, v in s:
        ...     d[k].append(v)
        ...
        >>> d.items()
        [('blue', [2, 4]), ('red', [1]), ('yellow', [1, 3])]
        """

        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values


def top_keywords(s, query, emotions_only=True, top=1000):
    keywordswithcount = keywords(s, query, emotions_only)
    keywordswithcount.sort(key=operator.itemgetter(1))
    keywordswithcount.reverse()

    return keywordswithcount[:top]


def not_low_freq_keywords(s, query, emotions_only=True, larger_than=3):
    keywordswithcount = keywords(s, query, emotions_only)
    keywordswithcount = [x for x in keywordswithcount if x[1] > larger_than]
    return keywordswithcount


def keywords(s, query, emotions_only):
    _scws = load_scws()
    emotion_words = set(load_emotion_words())

    results = s.search(query=query, max_offset=1000000000, fields=['text'])
    print results['hits']
    origin_data = []
    for r in results['results']:
        text = r['text'].encode('utf-8')
        words = [token[0] for token in _scws.participle(text) if token[0].isalnum() or len(token[0]) > 3]
        if emotions_only and 'text' in query and query['text']:
            if isinstance(query['text'], basestring):
                query_emotion = query['text'].encode('utf-8')
            elif hasattr(query['text'], '__getitem__'):
                query_emotion = query['text'][0].encode('utf-8')
            query_emotion = '[%s]' % query_emotion

            if (set(words) & emotion_words) and query_emotion in text:
                origin_data.append(words)
        else:
            origin_data.append(words)

    print 'mapreduce begin: ', str(time.strftime("%H:%M:%S", time.gmtime()))
    mapper = SimpleMapReduce(addcount2keywords, count_words)
    keywordswithcount = mapper(origin_data)
    print 'mapreduce end: ', str(time.strftime("%H:%M:%S", time.gmtime()))

    return keywordswithcount


def addcount2keywords(keywords):
    keywordswithcount = []
    for keyword in keywords:
        keywordswithcount.append((keyword, 1))
    return keywordswithcount


def count_words(item):
    word, occurances = item
    return (word, sum(occurances))


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


def load_emotion_words():
    emotion_words = [line.strip('\r\n') for line in file(EXTRA_EMOTIONWORD_PATH)]
    return emotion_words


def load_one_words():
    one_words = [line.strip('\r\n') for line in file(EXTRA_ONE_WORD_WHITE_LIST_PATH)]
    return one_words


single_word_whitelist = set(load_one_words())
single_word_whitelist |= set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')


def cut(s, text):
    return [token[0] for token
            in s.participle(text)
            if 3 < len(token[0]) < 30 or token[0] in single_word_whitelist]

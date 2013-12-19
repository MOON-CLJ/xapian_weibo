# -*- coding: utf-8 -*-

#  gathering snmp data
from __future__ import division
import re
import opencc
import os
from gensim import corpora
import cPickle as pickle
from xapian_weibo.utils import load_scws, cut, load_emotion_words

AB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

cut_str = load_scws()

cc = opencc.OpenCC('s2t', opencc_path='/usr/bin/opencc')
emotions_words = load_emotion_words()
emotions_words = [unicode(e, 'utf-8') for e in emotions_words]
t_emotions_words = [cc.convert(e) for e in emotions_words]
emotions_words.extend(t_emotions_words)
emotions_words = [w.encode('utf-8') for w in emotions_words]
emotions_words_set = set(emotions_words)
emotion_pattern = re.compile(r'\[(\S+?)\]')


def if_emoticoned_weibo(r):
    # 微博是否包含指定的表情符号集
    emotions = re.findall(emotion_pattern, r['text'])
    is_emoticoned = 1 if set(emotions) & emotions_words_set else 0
    return is_emoticoned


def if_empty_retweet_weibo(r):
    #暂时没用到该函数，故不作过多考虑
    is_empty_retweet = 1 if 'retweeted_status' in r and r['retweeted_status'] and r['text'] in [u'转发微博', u'轉發微博', u'Repost', u'Repost Weibo'] else 0
    return is_empty_retweet


'''define 3 kinds of seed emoticons'''
zan_set = set()
angry_set = set()
sad_set = set()


with open(os.path.join(AB_PATH, '4groups.csv')) as f:
    for l in f:
        pair = l.rstrip().split('\t')
        if pair[1] == '1' or pair[1] == '4':
            zan_set.add(pair[0].decode('utf-8', 'ignore'))

        if pair[1] == '2':
            angry_set.add(pair[0].decode('utf-8', 'ignore'))

        if pair[1] == '3':
            sad_set.add(pair[0].decode('utf-8', 'ignore'))

HAPPY = 1
ANGRY = 2
SAD = 3


def emoticon(text):
    """ Extract emoticons and define the overall sentiment """

    remotions = re.findall(emotion_pattern, text)
    zan = 0
    angry = 0
    sad = 0
    state = 0

    for e in remotions:
        if not zan and e in zan_set:
            zan = 1
        elif not angry and e in angry_set:
            angry = 1
        elif not sad and e in sad_set:
            sad = 1

        # 优化
        if zan + angry + sad > 1:
            return state

    zan_angry_sad = (zan, angry, sad)
    if zan_angry_sad == (1, 0, 0):
        state = HAPPY
    elif zan_angry_sad == (0, 1, 0):
        state = ANGRY
    elif zan_angry_sad == (0, 0, 1):
        state = SAD

    return state


'''define subjective dictionary and subjective words weight'''
dictionary_1 = corpora.Dictionary.load(os.path.join(AB_PATH, 'triple_subjective_1.dict'))
step1_score = {}
with open(os.path.join(AB_PATH, 'triple_subjective_1.txt')) as f:
    for l in f:
        lis = l.rstrip().split()
        step1_score[int(lis[0])] = [float(lis[1]), float(lis[2])]

'''define polarity dictionary and polarity words weight'''
dictionary_2 = corpora.Dictionary.load(os.path.join(AB_PATH, 'triple_polarity_1.dict'))
step2_score = {}
with open(os.path.join(AB_PATH, 'triple_polarity_1.txt')) as f:
    for l in f:
        lis = l.rstrip().split()
        step2_score[int(lis[0])] = [float(lis[1]), float(lis[2]), float(lis[3])]


def triple_classifier(tweet):
    sentiment = 0
    text = tweet['text']  # encode

    #if_empty_retweet = if_empty_retweet_weibo(tweet)
    #if if_empty_retweet:
    #    text = tweet['retweeted_status']['text']

    # if_emoticoned = if_emoticoned_weibo(tweet)
    # if if_emoticoned == 1:
    emoticon_sentiment = emoticon(text)
    if emoticon_sentiment != 0:
        sentiment = emoticon_sentiment
        text = u''

    if text != u'':
        entries = cut(cut_str, text.encode('utf-8'))
        entry = [e.decode('utf-8', 'ignore') for e in entries]
        bow = dictionary_1.doc2bow(entry)
        s = [1, 1]
        for pair in bow:
            s[0] = s[0] * (step1_score[pair[0]][0] ** pair[1])
            s[1] = s[1] * (step1_score[pair[0]][1] ** pair[1])
        if s[0] <= s[1]:
            bow = dictionary_2.doc2bow(entry)
            s = [1, 1, 1]
            for pair in bow:
                s[0] = s[0] * (step2_score[pair[0]][0] ** pair[1])
                s[1] = s[1] * (step2_score[pair[0]][1] ** pair[1])
                s[2] = s[2] * (step2_score[pair[0]][2] ** pair[1])
            if s[0] > s[1] and s[0] > s[2]:
                sentiment = HAPPY
            elif s[1] > s[0] and s[1] > s[2]:
                sentiment = SAD
            elif s[2] > s[1] and s[2] > s[0]:
                sentiment = ANGRY

    return sentiment

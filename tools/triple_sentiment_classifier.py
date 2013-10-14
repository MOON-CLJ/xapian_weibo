# -*- coding: utf-8 -*-

#  gathering snmp data
from __future__ import division
import os
import datetime
import random
import time
import nltk
import re
from gensim import corpora, models, similarities
import string
import cPickle as pickle
import leveldb
from xapian_weibo.utils import load_scws
from xapian_weibo.utils import cut

LEVELDBPATH = '/home/mirage/leveldb'
weibo_emoticoned_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emoticoned'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_empty_retweet_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_empty_retweet'),
                                             block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

cut_str = load_scws()

HAPPY = 1
ANGRY = 2
SAD = 3


def emoticon(zan_set, angry_set, sad_set, text):
    """ Extract emoticons and define the overall sentiment"""

    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)
    zan = 0
    angry = 0
    sad = 0

    if remotions:
        for e in remotions:
            if e in zan_set:
                zan = 1
            elif e in angry_set:
                angry = 1
            elif e in sad_set:
                sad = 1
    state = 0
    if zan == 1 and angry == 0 and sad == 0:
        state = HAPPY
    elif zan == 0 and angry == 1 and sad == 0:
        state = ANGRY
    elif zan == 0 and angry == 0 and sad == 1:
        state = SAD

    return state

'''define 3 kinds of seed emoticons'''
zan = set()
angry = set()
sad = set()

with open('/home/mirage/sentiment/4groups.csv') as f:
    for l in f:
        pair = l.rstrip().split('\t')
        if pair[1] == '1' or pair[1] == '4':
            zan.add(pair[0].decode('utf-8'))

        if pair[1] == '2':
            angry.add(pair[0].decode('utf-8'))

        if pair[1] == '3':
            sad.add(pair[0].decode('utf-8'))

'''define subjective dictionary and subjective words weight'''
dictionary_1 =corpora.Dictionary.load('/home/mirage/sentiment/subjective_54W_4.dict')
step1_score = {}
with open('/home/mirage/sentiment/new_emoticon_54W_4.txt') as f:
    for l in f:
        lis = l.rstrip().split()
        step1_score[int(lis[0])] = [float(lis[1]),float(lis[2])]

'''define polarity dictionary and polarity words weight'''
f = file('/home/mirage/sentiment/triple_sentiment.pkl', 'r')
dictionary_2 = pickle.load(f)
f.close()

step2_score = {}
with open('/home/mirage/sentiment/triple_sentiment_words_weight.txt') as f:
    for l in f:
        try:
            lis = l.rstrip().split()
            step2_score[int(lis[0])] = [float(lis[1]), float(lis[2]), float(lis[3])]
        except:
            print l


def triple_classifier(tweet):
    sentiment = 0
    text = tweet['text']  ##encode
    id_str = str(tweet['_id'])
    

    # if tweet['text'] == '转发微博':  ##转发微博
    #     text = tweet['retweeted_status']['text'].encode('utf-8')
    #     sentiment = 0
    #     text = ''
    if_empty_retweet = weibo_empty_retweet_bucket.Get(id_str)
    if if_empty_retweet:
        if_empty_retweet = int(if_empty_retweet)
    if if_empty_retweet == 1:
        mid_id_str = str(tweet['retweeted_status'])
    else:
        mid_id_str = id_str

    

    if_emoticoned = None
    try:
        if_emoticoned = weibo_emoticoned_bucket.Get(mid_id_str)
    except KeyError:
        
        misskey_err_count += 1

    # if if_emoticoned:
    #     if_emoticoned = int(if_emoticoned)
    # if if_emoticoned == 1:
    emoticon_sentiment = emoticon(zan, angry, sad, text)
    if emoticon_sentiment in [1,2,3]:
        sentiment = emoticon_sentiment
        text = ''
       

    if text != '':
        entries = cut(cut_str, text)
        entry = [e.decode('utf-8') for e in entries]
        bow = dictionary_1.doc2bow(entry)
        s = [1,1]
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
                sentiment = ANGRY
            elif s[2] > s[1] and s[2] > s[0]:
                sentiment = SAD

    return sentiment
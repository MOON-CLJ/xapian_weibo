#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import xapian
import simplejson as json
from collections import Counter
import collections
import itertools
import multiprocessing

class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values

def hasharr_to_list(keywords_hash):
        keywords_list = []
        for keyword in keywords_hash:
            keywords_list.append((keyword,keywords_hash[keyword]))
        return keywords_list

def count_words(item):
        word, occurances = item
        return (word, sum(occurances))

class WeiboSearch(object):
    def __init__(self, dbpath='simple'):
        database = xapian.Database(dbpath)
        enquire = xapian.Enquire(database)
        qp = xapian.QueryParser()
        stemmer = xapian.Stem("english")
        qp.set_stemmer(stemmer)
        qp.set_database(database)
        qp.set_stemming_strategy(xapian.QueryParser.STEM_SOME)
        self.qp = qp
        self.enquire = enquire
        self.emotionvi = 0
        self.keywordsvi = 1
        self.timestampvi = 2
        #loctvi = 3
        #reploctvi = 4
        #emotiononlyvi = 5
        #usernamevi = 6
        self.hashtagsvi = 7
        #uidvi = 8
        #repnameslistvi = 9
        self.maxitems = 1000000000

    def query(self,querystring=None,qtype=None,begin=None,end=None,keywords=[],hashtags=[]):
        if qtype == 'hy':
            query = self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.timestampvi, ''))
            querystring = begin+'..'+end
            query = self.qp.parse_query(querystring)
            print "Parsed query is: %s" % [str(query)]

            self.enquire.set_query(query)
            matches = self.enquire.get_mset(0,self.maxitems)

            # Display the results.
            print "%i results found." % matches.size()

            emotions_set = set()
            #keywords_counter = Counter()
            keywords_arr = []
            for m in matches:
                #emotion
                emotions_set = emotions_set | set(m.document.get_value(self.emotionvi).split())
                #keywords
                keywords_hash = json.loads(m.document.get_value(self.keywordsvi))
                keywords_arr.append(keywords_hash)
                #keywords_counter += Counter(json.loads(m.document.get_value(self.keywordsvi)))

            mapper = SimpleMapReduce(hasharr_to_list, count_words)
            #keywords_arr = [{'haha':1,"haha":2},{'haha':3}]
            word_counts = mapper(keywords_arr)
            keywords_hash = {}
            for word,count in word_counts:
                if count > 3:
                    keywords_hash[word] = count
            #print keywords_counter
            return emotions_set,keywords_hash

        if qtype == 'yq':
            query = self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.timestampvi, ''))
            querystring = begin+'..'+end
            query = self.qp.parse_query(querystring)
            print "Parsed query is: %s" % [str(query)]

            self.enquire.set_query(query)
            #matches = self.enquire.get_mset(0,1)
            matches = self.enquire.get_mset(0,self.maxitems)

            # Display the results.
            print "%i results found." % matches.size()

            keywords_arr = []
            for m in matches:
                #hashtag
                hashtags = json.loads(m.document.get_value(self.hashtagsvi))

                #keywords
                keywords_hash = json.loads(m.document.get_value(self.keywordsvi))
                keywords_arr.append(keywords_hash)
                #keywords_counter += Counter(json.loads(m.document.get_value(self.keywordsvi)))

            mapper = SimpleMapReduce(hasharr_to_list, count_words)
            word_counts = mapper(keywords_arr)
            keywords_hash = {}
            for word,count in word_counts:
                keywords_hash[word] = count
            #print keywords_counter
            return hashtags,keywords_hash

#test
search = WeiboSearch()
"""
emotions,keywords_hash = search.query(begin='0',end='131113198690',qtype='hy')
print 'emotions',emotions
#print 'keywords_hash',keywords_hash
"""
hashtags,keywords_hash = search.query(begin='0',end='131113198690',qtype='yq')
print 'hashtags',hashtags
print 'keywords_hash',keywords_hash
for keyword in keywords_hash:
    print keyword


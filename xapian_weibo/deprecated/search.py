#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  !此文件不再使用，重构中

#import sys
import xapian
import time
import datetime
import simplejson as json
#from collections import Counter
import collections
import itertools
import multiprocessing
import gc
import psutil
import redis


"""
mapper = SimpleMapReduce(hasharr_to_list, count_words)
        word_counts = mapper(lowkeywords_arr)
        lowkeywords_set = set()
        for word, count in word_counts:
            if count <= 3:
                lowkeywords_set.add(word)
"""


class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        num_workers = multiprocessing.cpu_count() * 2
        self.pool = multiprocessing.Pool(num_workers, maxtasksperchild=10000)
        #self.pool = multiprocessing.Pool(num_workers)

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


def hasharr_to_list(keywords_arr):
        keywords_list = []
        for keyword in keywords_arr:
            keywords_list.append((keyword, keywords_arr[keyword]))
        return keywords_list


def count_words(item):
        word, occurances = item
        return (word, sum(occurances))


class WeiboSearch(object):
    def __init__(self, dbpath='simplehaha'):
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
        self.loctvi = 3
        self.reploctvi = 4
        self.emotiononlyvi = 5
        #usernamevi = 6
        self.hashtagsvi = 7
        #uidvi = 8
        #repnameslistvi = 9
        #widvi = 10
        self.maxitems = 1000000000

        pool = redis.ConnectionPool(host='localhost', port=6379, db=1)
        self.r = redis.Redis(connection_pool=pool)
        self.r.flushdb()
        self.lowkeywords_set_rds = 'lowkeywords'

        pool1 = redis.ConnectionPool(host='localhost', port=6379, db=2)
        self.r1 = redis.Redis(connection_pool=pool1)
        self.r1.flushdb()
        self.keywords_hash_rds = 'keywords_hash'

    def lowkeywords_proc(self, matches):
        gc.disable()

        with self.r1.pipeline() as pipe:
            pipe_prep_count = 0
            pipe_size = 10000
            pipe.multi()
            for m in matches:
                for word, count in json.loads(m.document.get_value(self.keywordsvi)).items():
                    pipe.hincrby(self.keywords_hash_rds, word, count)
                pipe_prep_count += 1
                if pipe_prep_count % pipe_size == 0:
                    print '<----------------', pipe_prep_count
                    print 'keywords_hash mapreduce begin', psutil.phymem_usage()[1] / (1024 * 1024), 'M'
                    print 'mapreduce begin: ', str(time.strftime("%H:%M:%S", time.gmtime()))
                    pipe.execute()
                    pipe.reset()
                    print 'keywords_hash mapreduce end', psutil.phymem_usage()[1] / (1024 * 1024), 'M'
                    print 'mapreduce end: ', str(time.strftime("%H:%M:%S", time.gmtime()))

        with self.r1.pipeline() as pipe:
            pipe_prep_count = 0
            pipe_size = 10000
            pipe.multi()
            for word, count in self.r1.hgetall(self.keywords_hash_rds).items():
                pipe_prep_count += 1
                if count <= 3:
                    self.r.sadd(self.lowkeywords_set_rds, word)
                else:
                    pipe.hdel(self.keywords_hash_rds, word)
                if pipe_prep_count % pipe_size == 0:
                    print '<----------------', pipe_prep_count
                    pipe.execute()
                    pipe.reset()
                    print 'keywords_hash clean', psutil.phymem_usage()[1] / (1024 * 1024), 'M'

        gc.enable()
        self.r1.flushdb()
        print 'keywords_hash clean', psutil.phymem_usage()[1] / (1024 * 1024), 'M'

        return True

    def keywords_and_emotions_list_proc(self, matches):
        gc.disable()
        emotions_list = []
        keywords_list = []
        for m in matches:
            #emotion
            emotions_list.append(m.document.get_value(self.emotionvi).split())
            #keywords
            keywords_hash = json.loads(m.document.get_value(self.keywordsvi))
            per_keywords_list = []
            for word in keywords_hash:
                if self.r.sismember(self.lowkeywords_set_rds, word):
                    per_keywords_list.extend([word] * keywords_hash[word])
            keywords_list.append(per_keywords_list)
        gc.enable()
        self.r.flushdb()
        print 'lowkeywords_set clean', psutil.phymem_usage()[1] / (1024 * 1024), 'M'

        return emotions_list, keywords_list

    def query(self, querystring=None, qtype=None, begin=None, end=None, keywords=[], hashtags=[], synonymslist=[], emotiononly=False):
        if qtype == 'hy':
            self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.timestampvi, ''))
            querystring = begin + '..' + end

            if emotiononly:
                self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.emotiononlyvi, 'f', False))
                querystring += ' 1.0..1.0f'

            query = self.qp.parse_query(querystring)
            print "Parsed query is: %s" % [str(query)]

            self.enquire.set_query(query)
            #matches = self.enquire.get_mset(0, self.maxitems)
            matches = self.enquire.get_mset(0, 10000)
            # Display the results.
            print "%i results found." % matches.size()

            if not self.lowkeywords_proc(matches):
                return
            emotions_list, keywords_list = self.keywords_and_emotions_list_proc(matches)

            return emotions_list, keywords_list

        if qtype == 'yq':
            self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.timestampvi, ''))
            querystring = begin + '..' + end
            query = self.qp.parse_query(querystring)
            print "Parsed query is: %s" % [str(query)]

            self.enquire.set_query(query)
            #matches = self.enquire.get_mset(0,10)
            matches = self.enquire.get_mset(0, self.maxitems)

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

            print 'mapreduce begin: ', str(time.strftime("%H:%M:%S", time.gmtime()))
            mapper = SimpleMapReduce(hasharr_to_list, count_words)
            word_counts = mapper(keywords_arr)
            keywords_hash = {}
            for word, count in word_counts:
                keywords_hash[word] = count
            for synonyms in synonymslist:
                if len(synonyms) >= 2 and synonyms[0] in keywords_hash:
                    for word in synonyms[1:]:
                        if word in keywords_hash:
                            keywords_hash[synonyms[0]] += keywords_hash[word]
                            del keywords_hash[word]
            print 'mapreduce end: ', str(time.strftime("%H:%M:%S", time.gmtime()))

            #print keywords_counter
            return hashtags, keywords_hash

        if qtype == 'lh':
            self.qp.add_valuerangeprocessor(xapian.NumberValueRangeProcessor(self.timestampvi, ''))
            timequerystr = begin + '..' + end
            timequery = self.qp.parse_query(timequerystr)

            hashtags = ['H' + hashtag.lower() for hashtag in hashtags]
            keywords = [keyword.lower() for keyword in keywords]
            keywords.extend(hashtags)
            if len(keywords) > 0:
                wordsquery = xapian.Query(xapian.Query.OP_OR, keywords)
            else:
                return None

            query = xapian.Query(xapian.Query.OP_AND, [timequery, wordsquery])
            print "Parsed query is: %s" % [str(query)]

            self.enquire.set_query(query)
            self.enquire.set_sort_by_value(self.timestampvi, False)
            #matches = self.enquire.get_mset(0,10)
            matches = self.enquire.get_mset(0, self.maxitems)

            # Display the results.
            print "%i results found." % matches.size()

            results = []
            for m in matches:
                result = {}
                result['location'] = m.document.get_value(self.loctvi)
                result['repost_location'] = m.document.get_value(self.reploctvi)
                result['timestamp'] = xapian.sortable_unserialise(m.document.get_value(self.timestampvi))
                results.append(result)

            return results

"""
#test
search = WeiboSearch()

timenow = datetime.datetime.now()
begin = str(time.mktime((timenow + datetime.timedelta(days=-4000)).timetuple()))
end = str(time.mktime(timenow.timetuple()))

emotions, keywords_list = search.query(begin=begin, end=end, qtype='hy', emotiononly=True)
print psutil.phymem_usage()[1] / (1024 * 1024), 'M'
print gc.isenabled()

#print 'emotions', emotions
#print 'keywords_list', keywords_list
"""

"""
hashtags,keywords_hash = search.query(begin='0',end='131113198690',qtype='yq')
#print 'hashtags',hashtags
#print 'keywords_hash',keywords_hash
for keyword in keywords_hash:
    print keyword
"""
"""
timenow = datetime.datetime.now()
begin = str(time.mktime((timenow + datetime.timedelta(days=-8)).timetuple()))
end = str(time.mktime(timenow.timetuple()))

#print search.query(begin=begin,end=end,qtype='lh',keywords=['RUNANDRUN'],hashtags=['Runningman'])
for i in search.query(begin=begin,end=end,qtype='lh',keywords=['haha'],hashtags=['Runningman']):
    print i['timestamp']
"""

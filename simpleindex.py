#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import xapian
import string
import simplejson as json
#import ictclas
import re
import pymongo
import scws
import opencc

#print 'ictclas import', ictclas.ict_init("./")
#print 'ictclas import userdict', ictclas.import_dict('userdic.txt')
s = scws.Scws()
s.set_charset('utf-8')
s.set_dict('/usr/local/scws/etc/dict.utf8.xdb',scws.XDICT_MEM)
s.add_dict('userdic.txt',scws.XDICT_TXT)
s.set_rules('/usr/local/scws/etc/rules.utf8.ini')
s.set_ignore(1)

cc = opencc.OpenCC('mix2s')

#connection = pymongo.Connection()

connection = pymongo.Connection('219.224.135.60',27017)
db = connection.admin
db.authenticate('root','root')
db = connection.weibo
print 'pymongo success'

#stopwords
stopwords = set([line.strip('\r\n') for line in file('ext_stopword.dic')])
#emotionlist
emotionlist = [unicode(line.strip('\r\n'),'utf-8') for line in file('emotionlist.txt')]

if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: %s PATH_TO_DATABASE" % sys.argv[0]
    sys.exit(1)

try:
    # Open the database for update, creating a new database if necessary.
    database = xapian.WritableDatabase(sys.argv[1], xapian.DB_CREATE_OR_OPEN)
    print database,'open database weibo'
    emotionvi = 0
    keywordsvi = 1
    timestampvi = 2
    loctvi = 3
    reploctvi = 4
    emotiononlyvi = 5
    usernamevi = 6
    hashtagsvi = 7
    uidvi = 8
    repnameslistvi = 9
    widvi = 10

    """
    weibos = ''
    with open("statuses.js") as f:
        for line in f:
            weibos = json.loads(line)
    """
    count = 0
    for weibo in db.target_statuses.find():
    #for weibo in weibos:
        #init
        count += 1
        if count % 100000 == 0:
            print '<------------------>'
            print count
        indexer = xapian.TermGenerator()
        stemmer = xapian.Stem("english")
        indexer.set_stemmer(stemmer)
        doc = xapian.Document()
        indexer.set_document(doc)

        #-->username
        username = weibo['name']
#        print 'username',username
        doc.add_value(usernamevi,username)

        #-->uid
        uid = weibo['uid']
        #indexer.index_text(uid,1,'I')
        doc.add_value(uidvi,uid)

        #-->wid
        wid = weibo['_id']
        doc.add_value(widvi,wid)

        #-->text
        text = weibo['text'].lower()
        try:
            text += ' ' + weibo['repost']['text'].lower()
        except:
            pass
        text = cc.convert(text)
#        print 'orginal',text
        #content
        #doc.set_data(weibo['text'])

        #repostnameslist
        repnames_arr = []
        for username in re.findall(r'//@(\S+?):', text):
            if username not in repnames_arr:
                repnames_arr.append(username)
        try:
            repostname = weibo['repost']['username']
            repnames_arr.append(repostname)
        except:
            pass
        repnames = json.dumps(repnames_arr, ensure_ascii = False)
#        print 'repostnameslist',repnames
        doc.add_value(repnameslistvi,repnames)

        #@user
        usernames = u''
        for username in re.findall(u"@([\u2E80-\u9FFFA-Za-z0-9_-]+) ?", text):
            usernames += (username + u" ")
        text = re.sub(u"@([\u2E80-\u9FFFA-Za-z0-9_-]+) ?"," ",text)
        #usernames = usernames.encode('utf-8')
 #       print 'usernames',usernames
        #indexer.index_text(usernames,1,'U')

        #hashtag
        hashtags_arr = []
        for hashtag in re.findall(r"#(.+?)#", text):
            hashtag = hashtag.encode('utf-8')
            if hashtag not in hashtags_arr:
                hashtags_arr.append(hashtag)
        #text = re.sub(u"#([\u2E80-\u9FFFA-Za-z0-9_-]+)# ?"," ",text)
        hashtags = ' '.join(hashtags_arr)
#        hashtags = hashtags.encode('utf-8')
#        print 'hashtags',hashtags
        indexer.index_text(hashtags,1,'H')

        #emotion
        emotions_arr = []
        for emotion in re.findall(r"\[(\S+?)\]", text):
            if emotion not in emotions_arr and emotion in emotionlist:
                emotions_arr.append(emotion)
        text = re.sub(r"\[\S+?\]"," ",text)
        emotions = ' '.join(emotions_arr)
        emotions = emotions.encode('utf-8')
#        print 'emotions',emotions
        #indexer.index_text(emotions,1,'E')
        doc.add_value(emotionvi,emotions)

        #emotiononly
        if len(emotions_arr) == 1:
 #           print 'emotiononly',1
            doc.add_value(emotiononlyvi,xapian.sortable_serialise(1))
        else:
 #           print 'emotiononly',0
            doc.add_value(emotiononlyvi,xapian.sortable_serialise(0))

        #short url
        text = re.sub(r"http://t\.cn/[-\w]+"," ",text)

        #token
        text = text.encode('utf-8')
        #tokens = ictclas.process_str(text,0)
        #tokens_arr = [token for token in tokens.split() if token not in stopwords]
        tokens_arr = [token[0] for token in s.participle(text) if token[0] not in stopwords]
        tokens = ' '.join(tokens_arr)
#        print 'tokens',tokens
        indexer.index_text(tokens)

        #terms
        #temp = ''
        #for term in indexer.get_document().termlist():
        #    temp += term.term + str(term.wdf)+ ' '
        #print 'terms',temp

        #hashtags arr
        hashtags_str = json.dumps(hashtags_arr,ensure_ascii = False)
#        print 'hashtags_str',hashtags_str.encode('utf-8')
        doc.add_value(hashtagsvi,hashtags_str)

        #keywords
        keywords_hash = {}
        for term in indexer.get_document().termlist():
            keyword = term.term.lstrip('IUHEZ')
            if keyword not in hashtags_arr and keyword not in keywords_hash:
                keywords_hash[keyword] = term.wdf
        keywords = json.dumps(keywords_hash, ensure_ascii = False)
#        print 'keywords',keywords.encode('utf-8')
        doc.add_value(keywordsvi,keywords)

        #-->ts
        timestamp = weibo['ts']
#        print 'timestamp',timestamp
        doc.add_value(timestampvi,xapian.sortable_serialise(timestamp))

        #-->location
        location = weibo['location']
#        print 'location',location
        doc.add_value(loctvi,location)

        #-->repost location
        try:
            repost_location = weibo['repost']['location']
        except:
            repost_location = ''
#        print 'repost_location',repost_location
        doc.add_value(reploctvi,repost_location)

        # Add the document to the database.
        database.add_document(doc)

    database.flush()
except Exception, e:
    print >> sys.stderr, "Exception: %s" % str(e)
    sys.exit(1)

#print 'ictclas exit',ictclas.ict_exit()


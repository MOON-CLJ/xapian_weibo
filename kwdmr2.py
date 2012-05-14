# -*- coding: cp936 -*-
import pymongo
import time
from pymongo import Connection
#import nltk,re
#from tokenizer import smallseg
from operator import itemgetter
from simplesearch import WeiboSearch

print 'haha'
"""
connection = pymongo.Connection()
db = connection.admin
db.authenticate('root', 'root')
db = connection.weibo
from bson.code import Code

topkwd=40##number of hot keywords
tperiod=60*60*12##time intervals for statistics
tcurrent=time.time()##system currenttime
tstart=tcurrent-tperiod##latest time intervals starting time
tlstart=tcurrent-2*tperiod##last time intervals starting time
lastresult=db.kwdstatistics
lastkwd={}
t0=time.time()
print lastresult.count()
print 'mapreduce begin at '+str(t0)
for i in lastresult.find():
    lastkwd[i['_id']]=i['value']
    #print i['_id']
    print lastkwd[i['_id']]
mapperlatest=Code('''
            function(){
                this._keywords.forEach(function(z){
                    emit(z,1);
                });
            }
            ''')
reducerlatest=Code('''
            function(key,values){
                var total=0;
                for (var i=0;i<values.length;i++){
                    total+=values[i];
                }
                return total;
            }
            ''')
db.kwdstatistics.remove()
kwdlatest=db.target_statuses.map_reduce(mapperlatest,reducerlatest,'kwdstatistics',query={'ts':{'$gte':tstart},'ts':{'$lte':tcurrent}})
t1=time.time()
print 'mapreduce end at '+str(t1)+' time used '+str(t1-t0)
kwdincrse=[]##this list used to record keywords used times increasing rate
sortedincrse=[]##this list used to record sorted keywords used times increasing rate
stopwords=smallseg.SEG().stopwords
tuplefile=open('tuple.txt','w')
t2=time.time()
print 'increaserating begin at '+str(t2)
search = Search()
hashtags,keywords_hash = search.query(begin='0',end='131113198690',qtype='yq')
p
for keyword in keywords_hash:
    if keyword in lastkwd:
        if len(keyword)<8 and not keyword.isdigit():
            ##print kwdcount['_id']
            latestamount=keywords_hash[keyword]
            lastamount=lastkwd[keyword]
            ##print str(kwdcount['value'])+','+str(kwdcount2['value'])
            incrserate=(latestamount-lastamount)/lastamount
            kwdincrse.append(tuple([kwdcount['_id'],incrserate]))
            tuplefile.write(kwdcount['_id'].encode('gbk')+':'+str(lastamount)+','+str(latestamount)+','+str(incrserate)+'\n')
t3=time.time()
print 'increaserating end at '+str(t3)+' time used'+str(t3-t2)
t4=time.time()
print 'sorting begin at '+str(t4)
sortedincrse=sorted(kwdincrse, key=itemgetter(1), reverse=True)
t5=time.time()
print 'sorting end at '+str(t5)+' time used '+str(t5-t4)
count=0
hotkwdlist=[]
for skwd in sortedincrse:##to find topk hot kwds
    count=count+1
    if count<=topkwd:
        hotkwdlist.append(skwd[0])
        print skwd[0].encode('utf-8')
##microblog=db.test
##qperiod=tcurrent-3600*24##the whole day mocroblogs that cover hot kwds
##forpatterns=open('forpatterns.txt','w')
##count=0
##mblogid=0
#for i in microblog.find({'_keywords':hotkwdlist,'ts':{'$gte':qperiod},'ts':{'$lte':tcurrent}}):
##for i in microblog.find({'ts':{'$gte':qperiod},'ts':{'$lte':tcurrent}}):
    ##mblogid=mblogid+1
    ##print str(mblogid)
    ##for keyword in i['_keywords']:
        ##if keyword in hotkwdlist:
            ##count=count+1
            ##if count==1:
                ##forpatterns.write(str(mblogid)+' ')
                ##forpatterns.write(keyword.encode('utf-8'))
            ##else:
                ##forpatterns.write(' ')
                ##forpatterns.write(keyword.encode('utf-8'))
    ##forpatterns.write('\r\n')
"""

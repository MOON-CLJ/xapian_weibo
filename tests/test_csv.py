# -*- coding: UTF-8 -*-
import csv
import re
import os
import time
import datetime

def toJson(li):
    weibo = dict()
    if li[0]:
        weibo['user'] = li[0]
    else:
        weibo['user'] = 'None'
    
    if li[1]:
        weibo['retweeted_user'] = li[1]
    else:
        weibo['retweeted_user'] = 'None'

    if li[2]:
        weibo['_id'] = li[2]
    else:
        weibo['_id'] = 'None'

    if li[3]:
        weibo['retweeted_status'] = li[3]
    else:
        weibo['retweeted_status'] = 'None'

    if li[4]:
        data_time = datetime.datetime(1970,1,1)
        r_time = data_time + datetime.timedelta(seconds=int(li[4]))
        end_time = str(r_time.year) + '-' + str(r_time.month) + '-' + str(r_time.day) + ' ' + str(r_time.hour) + ':' + str(r_time.minute) + ':' + str(r_time.second)
        weibo['timestamp'] = time.mktime(time.strptime(str(r_time), '%Y-%m-%d %H:%M:%S'))
    else:
        weibo['timestamp'] = 'None'
        
    if li[5]:
        weibo['input_time'] = time.mktime(time.strptime(str(li[5]), '%Y-%m-%d %H:%M:%S'))
    else:
        weibo['input_time'] = 'None'

    if li[6]:
        weibo['ip'] = li[6]
    else:
        weibo['ip'] = 'None'
    
    if li[7]:
        weibo['province'] = li[7]
    else:
        weibo['province'] = 'None'
        
    if li[8]:
        weibo['city'] = li[8]
    else:
        weibo['city'] = 'None'

    if li[9]:
        weibo['message_type'] = li[9]
    else:
        weibo['message_type'] = 'None'

    if li[10]:
        weibo['followers_count'] = li[10]
    else:
        weibo['followers_count'] = 'None'

    if li[11]:
        weibo['friends_count'] = li[11]
    else:
        weibo['friends_count'] = 'None'

    if li[12]:
        weibo['comments_count'] = li[12]
    else:
        weibo['comments_count'] = 'None'

    if li[13]:
        weibo['reposts_count'] = li[13]
    else:
        weibo['reposts_count'] = 'None'

    if li[14]:
        weibo['retweeted_comments_count'] = li[14]
    else:
        weibo['retweeted_comments_count'] = 'None'

    if li[15]:
        weibo['retweeted_reposts_count'] = li[15]
    else:
        weibo['retweeted_reposts_count'] = 'None'

    if li[16]:
        weibo['text'] = li[16]
    else:
        weibo['text'] = 'None'

    if li[17]:
        weibo['is_long'] = li[17]
    else:
        weibo['is_long'] = 'None'

    if li[18]:
        weibo['pic_url'] = li[18]
    else:
        weibo['pic_url'] = 'None'

    if li[19]:
        weibo['pic_content'] = li[19]
    else:
        weibo['pic_content'] = 'None'

    if li[20]:
        weibo['audio_url'] = li[20]
    else:
        weibo['audio_url'] = 'None'

    if li[21]:
        weibo['audio_content'] = li[21]
    else:
        weibo['audio_content'] = 'None'

    if li[22]:
        weibo['video_url'] = li[22]
    else:
        weibo['video_url'] = 'None'

    if li[23]:
        weibo['video_content'] = li[23]
    else:
        weibo['video_content'] = 'None'

    return weibo
    

def yuan():
    with open('MB_QL_1111_1112_NODE1.csv') as f:
        n = 0
        i = 0
        for line in f:
            n = n + 1
            if n%1000 == 0:
                print n
            li = line.strip('\n').split(',')
            if len(li) == 25 and li[24] == '1':
                i = i + 1
                item = toJson(li)#将一条数据list转换成一条dict对象
                #print item

    f.close()
    print i,n
    print float(i)/float(n)

if __name__ == '__main__':
    yuan()

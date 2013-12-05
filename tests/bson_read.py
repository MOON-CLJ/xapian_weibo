# -*- coding: utf-8 -*-

import time
import csv
import json
import sys
import re
import urllib, urllib2
sys.path.append('../xapian_weibo')
from datetime import datetime, date
from bs_input import KeyValueBSONInput
from xml.dom import minidom
from xml.etree import ElementTree
from xml.etree.ElementTree import ElementTree, Element, SubElement, dump, Comment, tostring
from random import randrange

BSON_FILEPATH = "/opt/backup/mongodump/20130129/master_timeline/master_timeline_weibo.bson"

def generateIP():
##    rand_ip = [58,59,60,61,121,122,123,124,125,202,210,218,219,222]
##    indexOne = randrange(0, 14, 1)
##    blockOne = rand_ip[indexOne]
    blockOne = 202
    blockTwo = randrange(96, 112, 1)
    blockThree = randrange(0, 255, 1)
    blockFour = 0
    if blockOne == 10:
	return self.__generateRandomIP__()
    elif blockOne == 172:
	return self.__generateRandomIP__()
    elif blockOne == 192:
	return self.__generateRandomIP__()
    else:
	return str(blockOne) + '.' + str(blockTwo) + '.' + str(blockThree) + '.' + str(blockFour)

def IP_City(ip):
    url = 'http://api.map.baidu.com/location/ip?ak=E432f09c6597f75cc3cf55a71a685ae1&ip=%s&coor=bd09ll' % ip
    req = urllib2.Request(url)
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    try:
        res = json.loads(res)
    except ValueError:
        return 'Error'
    if 'content' not in res:
        return 'Error'
    else:
        return res['content']['address']

def load_bs(bs_filepath=BSON_FILEPATH):
    print 'bson file mode: 从备份的BSON文件中 %s 加载数据' % (bs_filepath)
    bs_input = KeyValueBSONInput(open(bs_filepath, 'rb'))
    return bs_input

def buildET(name,root,text):#建立某个节点
    item_data = Element(name)        
    try:
        item_data.text = str(text)
    except UnicodeEncodeError:
        item_data.text = text
    root.append(item_data)
    return root

def subET(data,name,root):#建立某个节点的子节点
    item_data = Element(name)
    root.append(item_data)
    SubElement(item_data, 'u_id').text = str(data['id'])
    SubElement(item_data, 'u_name').text = data['name']
    SubElement(item_data, 'u_gender').text = str(data['gender'])
    SubElement(item_data, 'u_province').text = data['province']
    SubElement(item_data, 'u_city').text = data['city']
    SubElement(item_data, 'u_location').text = data['location']
    SubElement(item_data, 'u_description').text = data['description']
    SubElement(item_data, 'u_verified').text = str(data['verified'])
    SubElement(item_data, 'u_followers_count').text = str(data['followers_count'])
    SubElement(item_data, 'u_statuses_count').text = str(data['statuses_count'])
    SubElement(item_data, 'u_friends_count').text = str(data['friends_count'])
    SubElement(item_data, 'u_profile_image_url').text = str(data['profile_image_url'])
    SubElement(item_data, 'u_bi_followers_count').text = str(data['bi_followers_count'])
    SubElement(item_data, 'u_verified_type').text = str(data['verified_type'])
    return root

def load_weibo_from_bs():
    bs_input = load_bs()
    h = 0
    f = open('jsonTOXML.xml', 'w')
    f.write('<?xml version="1.0" encoding="utf-8"?>'+'\n'+'<statuses>')
    
    for _id, weibo in bs_input.reads():
        id = _id
        created_at = weibo['created_at']
        text = weibo['text']
        source = weibo['source']
        reposts_count = weibo['reposts_count']
        comments_count = weibo['comments_count']
        attitudes_count = weibo['attitudes_count']
        geo = weibo['geo']

        if 'user' not in weibo:
            continue
        user = weibo['user']

        u_id = user['id']
        u_name = user['name']
        u_gender = user['gender']
        u_province = user['province']
        u_city = user['city']
        u_location = user['location']

        if 'description' not in user:
            continue
        u_description = user['description']

        u_verified = user['verified']
        u_followers_count = user['followers_count']
        u_statuses_count = user['statuses_count']
        u_friends_count = user['friends_count']
        u_profile_image_url = user['profile_image_url']
        u_bi_followers_count = user['bi_followers_count']

        if 'verified_type' not in user:
            continue
        u_verified_type = user['verified_type']

        if 'retweeted_status' not in weibo:
            continue

        
        h = h + 1
        print h
        if h > 4:
            break
        if not weibo['retweeted_status']:
            xml_data = ElementTree()
            root = Element('status')
            xml_data._setroot(root)

            root = buildET('id',root,id)
            root = buildET('created_at',root,created_at)
            root = buildET('text',root,text)
            root = buildET('source',root,source)
            root = buildET('reposts_count',root,reposts_count)
            root = buildET('comments_count',root,comments_count)
            root = buildET('attitudes_count',root,attitudes_count)
            root = buildET('geo',root,geo)

            weibo_ip = generateIP()#随机产生一个微博的IP地址
            weibo_city = IP_City(weibo_ip)#将随机产生的IP解析成对应的城市

            while weibo_city == 'Error':#判断IP是否存在
                weibo_ip = generateIP()
                weibo_city = IP_City(weibo_ip)

            root = buildET('IP',root,weibo_ip)            
            root = buildET('address',root,weibo_city)
            
            root = subET(user,'user',root)

            xml_data.write(f, 'utf-8')
            f.write('\n')

        else:
            if 'description' not in weibo['retweeted_status']['user']:
                continue
            if 'user' not in weibo['retweeted_status']:
                continue
            if 'verified_type' not in weibo['retweeted_status']['user']:
                continue
            xml_data = ElementTree()
            root = Element('status')
            xml_data._setroot(root)

            root = buildET('id',root,id)
            root = buildET('created_at',root,created_at)
            root = buildET('text',root,text)
            root = buildET('source',root,source)
            root = buildET('reposts_count',root,reposts_count)
            root = buildET('comments_count',root,comments_count)
            root = buildET('attitudes_count',root,attitudes_count)
            root = buildET('geo',root,geo)

            weibo_ip = generateIP()#随机产生一个微博的IP地址
            weibo_city = IP_City(weibo_ip)#将随机产生的IP解析成对应的城市

            while weibo_city == 'Error':#判断IP是否存在
                weibo_ip = generateIP()
                weibo_city = IP_City(weibo_ip)

            root = buildET('IP',root,weibo_ip)
            root = buildET('address',root,weibo_city)

            root = subET(user,'user',root)

            retweeted_data = Element('retweeted_status')
            root.append(retweeted_data)

##            retweeted_ip = generateIP()#随机产生一个微博的IP地址
##            retweeted__city = IP_City(retweeted__ip)#将随机产生的IP解析成对应的城市
##
##            while retweeted__city == 'Error':#判断IP是否存在
##                retweeted__ip = generateIP()
##                retweeted__city = IP_City(retweeted__ip)

            SubElement(retweeted_data, 'r_ip').text = weibo_ip
            SubElement(retweeted_data, 'r_address').text = weibo_city

            SubElement(retweeted_data, 'r_id').text = str(weibo['retweeted_status']['mid'])
            SubElement(retweeted_data, 'r_created').text = weibo['retweeted_status']['created_at']
            SubElement(retweeted_data, 'r_text').text = weibo['retweeted_status']['text']
            retweeted_data = subET(weibo['retweeted_status']['user'],'user',retweeted_data)

            xml_data.write(f, 'utf-8')
            f.write('\n')
                
    f.write('\n'+'</statuses>')
    f.close()


if __name__ == '__main__':
    load_weibo_from_bs()
        

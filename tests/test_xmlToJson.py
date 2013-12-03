# -*- coding:utf-8 -*-

import sys
import json
import re
import os
import xml.etree.ElementTree as ET
sys.path.append('../xapian_weibo')
from utils import xml_to_json

def load_xml(filename):

    json_data = []#用以存储json格式的数组
    weibo_item = {}
    item = {}
    for event, elem in ET.iterparse('%s.xml'%(filename)):
        if event == 'end':#匹配tag结束符来读取数据
            if elem.tag == 'status':#status表示一条微博记录
                
                status_items = elem.getchildren()
                for status_item in status_items:
                    weibo_item[status_item.tag] = item[status_item.tag]
                json_data.append(weibo_item)#表示加载一条json记录，涉及到大数据时候可以加载一条读一条
                print weibo_item
                
            else:
                item[elem.tag] =  xml_to_json(elem,item)
        elem.clear() # discard the element    


if __name__ == '__main__':
    load_xml("jsonTOXML")

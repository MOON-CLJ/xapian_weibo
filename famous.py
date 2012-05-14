# -*- coding: utf-8 -*-

from splinter.browser import Browser
import redis
import time
import re
from bs4 import BeautifulSoup
from fifo import WEIBOUidList 

browser = Browser('chrome', user_agent='Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.162 Safari/535.19')
url = "http://weibo.com"
SPROV = [['安徽','34'],['北京','11'],['重庆','50'],['福建','35'],['甘肃','62'],['广东','44'],['广西','45'],['贵州','52'],['海南','46'],['河北','13'],['黑龙江','23'],['河南','41'],['湖北','42'],['湖南','43'],['内蒙古','15'],['江苏','32'],['江西','36'],['吉林','22'],['辽宁','21'],['宁夏','64'],['青海','63'],['山西','14'],['山东','37'],['上海','31'],['四川','51'],['天津','12'],['西藏','54'],['新疆','65'],['云南','53'],['浙江','33'],['陕西','61'],['台湾','71'],['香港','81'],['澳门','82']]

#famous uids
famous_uids = WEIBOUidList()


#login
def login(username,password):
    print 'login'
    browser.visit(url)
    browser.find_by_id('loginname').fill(username)
    button = browser.find_by_id("login_submit_btn")
    while True:
        try:
            print 'focus'
            button.click()
            browser.find_by_id('password').first.click()
            browser.find_by_id('password').fill(password)
            break
        except Exception:
            continue
    button.click()

while True:
    try:
        login('lijunli2598@126.com', '563389034')
        break
    except Exception:
        continue

while len(browser.title) == 18:
    print 'wait for homepage'
    time.sleep(0.5)

for letter in range(97,123):
    for prov in SPROV:
        for pages in range(1,11):
            url = 'http://verified.weibo.com/fame/%s/?rt=4&srt=3&province=%s&page=%s' % (chr(letter),prov[1],str(pages))
            print url
            browser.visit(url)
            while not browser.is_element_present_by_css('.categories_list .titlebar', wait_time=10):
                print 'page reload'
                browser.reload()
            soup = BeautifulSoup(browser.html)
            soup = BeautifulSoup(str(soup.find('div',{'class':'detail'})))
            for i in soup.find_all('input'):
                print i['value']
                famous_uids.intappend(i['value'])
            soup = BeautifulSoup(browser.html)
            ifnext = soup.find('div',{'class':'W_pages W_pages_comment'})

            if str(ifnext).find('下一页') == -1:
                print 'no more page'
                break

# -*- coding: utf-8 -*-

from splinter.browser import Browser
import redis
import time
import re
from BeautifulSoup import BeautifulSoup
from fifo import WEIBOUidList

browser = Browser('chrome', user_agent='Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.162 Safari/535.19')
url = "http://weibo.com"
SPROV = [['安徽','34'],['北京','11'],['重庆','50'],['福建','35'],['甘肃','62'],['广东','44'],['广西','45'],['贵州','52'],['海南','46'],['河北','13'],['黑龙江','23'],['河南','41'],['湖北','42'],['湖南','43'],['内蒙古','15'],['江苏','32'],['江西','36'],['吉林','22'],['辽宁','21'],['宁夏','64'],['青海','63'],['山西','14'],['山东','37'],['上海','31'],['四川','51'],['天津','12'],['西藏','54'],['新疆','65'],['云南','53'],['浙江','33'],['陕西','61'],['台湾','71'],['香港','81'],['澳门','82']]

#login
def login(username,password):
    print 'login'
    browser.visit(url)

    browser.find_by_name('loginname').fill(username)
    button = browser.find_by_css(".W_login .W_btn_d").first
    while True:
        try:
            print 'focus'
            button.click()
            browser.find_by_name('password').click()
            browser.find_by_name('password').fill(password)
            break
        except Exception, e:
            print e
            continue
    button.click()

#goto club
def goto_club():
    url = 'http://club.weibo.com/toprank'
    print url
    browser.visit(url)

#daren uids
daren_uids = WEIBOUidList()

while True:
    try:
        login('lijunli2598@126.com', '563389034')
        break
    except Exception:
        continue

while len(browser.title) == 18:
    print 'wait for homepage'
    time.sleep(0.5)

goto_club()
while len(browser.title) != 5:
    print 'wait for club page'
    time.sleep(0.5)

for i in SPROV:
    while True:
        try:
            while not browser.is_element_present_by_css('.talent_rankinglist .tab .cur a', wait_time=10):
                print 'prov and city page reload'
                browser.reload()
            browser.select('sprov',i[1])
            break
        except:
            continue

    #city = browser.find_by_xpath('//select[@name="scity"]/option[@value="'+str(j)+'"]')
    soup = BeautifulSoup(browser.html)
    soup = BeautifulSoup(str(soup.find('select',{'name':'scity'})))
    citys = [[j.get('value'),j.string] for j in soup.findAll('option')]

    for city in citys:
        while True:
            try:
                while not browser.is_element_present_by_css('.talent_rankinglist .tab .cur a', wait_time=10):
                    print 'prov and city page reload'
                    browser.reload()
                browser.select('scity',city[0])
                break
            except:
                continue
        while not browser.is_element_present_by_css('.talent_rankinglist .tab .cur a', wait_time=10):
            print 'prov and city page reload'
            browser.reload()
        browser.click_link_by_text('查找')
        while not browser.is_element_present_by_css('.talent_rankinglist .tab .cur a', wait_time=10):
            print 'prov and city page reload'
            browser.reload()

        baseurl = browser.url
        for i in range(11)[1:]:
            url = baseurl + '&page=' + str(i) + '&'
            print url
            while True:
                try:
                    browser.visit(url)
                    break
                except Exception:
                    continue
            soup = BeautifulSoup(browser.html)
            ol = soup.find('ol')
            if ol is None:
                print 'no this page'
                break
            soup = BeautifulSoup(str(ol))
            for child in soup.findAll('a',{'class':'name'}):
                try:
                    print re.search(r'sinaimg.cn/([0-9]*)/',soup.find('img',{'title':child.string}).get('src')).group(1)
                    daren_uids.intappend(re.search(r'sinaimg.cn/([0-9]*)/',soup.find('img',{'title':child.string}).get('src')).group(1))
                except Exception:
                    pass
        print city[1].encode('utf-8') + ' is Done'


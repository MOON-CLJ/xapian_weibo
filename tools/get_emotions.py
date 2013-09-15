import urllib
import simplejson as json

res = urllib.urlopen('https://api.weibo.com/2/emotions.json?access_token=2.00z5vGCDyoqaVE13ca298d600LL9mR')
data = res.read()
data = json.loads(data)

with open('../dict/emotionlist.txt', 'w') as f:
    for d in data:
        f.write('%s\n' % d['value'][1:-1].encode('utf-8'))

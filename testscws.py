# -*- coding: utf-8 -*-
import time
"""
import scws

text = u'用髮膠都無得'
s = scws.Scws()
s.set_charset('utf-8')
s.set_dict('/usr/local/scws/etc/dict.utf8.xdb',scws.XDICT_MEM)
s.add_dict('userdic.txt',scws.XDICT_TXT)
s.set_rules('/usr/local/scws/etc/rules.utf8.ini')
s.set_ignore(1)

for word in s.participle(text.encode('utf-8')):
    print word[0]
"""
"""
s = set()
for i in range(20000000):
    s.add(i)

now = time.time()
print (10000 in s)
print time.time() - now
"""
import opencc
cc = opencc.OpenCC('mix2s')
print cc.convert(u'Open Chinese Convert（OpenCC）「开放開放中文轉換」，是一個致力於中文簡繁轉換的項目，提供高質量詞庫和函數庫(libopencc)。')


# -*- coding: utf-8 -*-
import time
import scws

text = '加加加加加加加加'  # * 55000

s = scws.Scws()
s.set_charset('utf-8')
s.set_dict('/usr/local/scws/etc/dict.utf8.xdb', scws.XDICT_MEM)
s.add_dict('/usr/local/scws/etc/dict_cht.utf8.xdb', scws.XDICT_MEM)
s.add_dict('../dict/userdic.txt', scws.XDICT_TXT)
s.add_dict('../dict/stopword.dic', scws.XDICT_TXT)
s.add_dict('../dict/emotionlist.txt', scws.XDICT_TXT)

s.set_rules('/usr/local/scws/etc/rules.utf8.ini')
s.set_ignore(1)

for word in s.participle(text):
    print word[0]

"""
import opencc
cc = opencc.OpenCC('mix2s')
print cc.convert(u'Open Chinese Convert（OpenCC）「开放c)。')
"""

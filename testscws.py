# -*- coding: utf-8 -*-

import scws

text = u'Hello, 我名字叫李那曲是一个中国人, 我有时买Q币来玩,我还听说过C++语言敢说敢做北京航空航天大学'
s = scws.Scws()
s.set_charset('utf-8')
s.set_dict('/usr/local/scws/etc/dict.utf8.xdb',scws.XDICT_MEM)
s.add_dict('userdic.txt',scws.XDICT_TXT)
s.set_rules('/usr/local/scws/etc/rules.utf8.ini')
s.set_ignore(1)

for word in s.participle(text.encode('utf-8')):
    print word[0]


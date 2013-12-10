# -*- coding: utf-8 -*-
from xapian_weibo.utils import load_scws

text = '加加加加加加加加'  # * 55000

text = '我们是好朋友'
s = load_scws()

for word in s.participle(text):
    print word[0]

import opencc
cc = opencc.OpenCC('mix2s', opencc_path='/usr/bin/opencc')
print cc.convert(u'Open Chinese Convert（OpenCC）「开放c)。')

f = open('stopword.dic', 'w')

for line in file('ext_stopword.dic'):
    f.write('%s 1 1 !\n' % line.strip())

f.close()

for line in file('stopword.dic'):
    print line

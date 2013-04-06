import bson
from input import KeyValueBSONInput


for i in file('april_weibo1.bson', 'rb'):
    print [i]
    #print bson.decode_all(i)
    break

bs_input = KeyValueBSONInput(open('april_weibo1.bson', 'rb'))
for i in bs_input.reads():
    print i[0], i[1].keys()
bs_input.close()

import pymongo
DB_USER = 'root'
DB_PWD = 'root'

connection = pymongo.Connection()
db = connection.admin
db.authenticate(DB_USER, DB_PWD)
db = connection.weibo

print db['statuses'].find({'ts': {'$gt': 1335493747}, 'ts': {'$lt': 1335493867}}).count()

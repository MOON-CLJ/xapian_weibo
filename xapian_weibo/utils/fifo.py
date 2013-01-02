import redis

class MessageQueue(object):
    def __init__(self, queue_name='uid_queue'):
        self.r = redis.Redis()
        self.queue_name = queue_name

    def put(self, value):
        self.r.rpush(self.queue_name, value)

    def get(self):
        return self.r.blpop(self.queue_name)[1]

    def empty(self):
        if not self.r.llen(self.queue_name):
            return True
        else:
            return False

class WEIBOUidList(object):
    def __init__(self, list_name='uid_list_realtime', hash_name='uid_hash_realtime', index_info='index_info_hash'):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.r = redis.Redis(connection_pool=pool)
        self.list_name = list_name
        self.hash_name = hash_name
        self.index_info = index_info

        if self.r.hget(index_info, 'left') is None:
            self.r.hset(index_info, 'left', 0)
        if self.r.hget(index_info, 'right') is None:
            #append start uid
            self.r.hset(index_info, 'right', self.count())

    def intappend(self, uid):
        with self.r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(self.hash_name)
                    count = pipe.hget(self.hash_name, uid)
                    if count is None:
                        pipe.multi()
                        pipe.hset(self.hash_name, uid, 1)
                        pipe.rpush(self.list_name, uid)
                        pipe.hincrby(self.index_info, 'right', 1)
                        pipe.execute()
                    else:
                        pipe.multi()
                        pipe.hincrby(self.hash_name, uid, 1)
                        pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

    def get(self):
        with self.r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(self.index_info)
                    left = pipe.hget(self.index_info, 'left')
                    right = pipe.hget(self.index_info, 'right')
                    if int(left) < int(right):
                        pipe.multi()
                        pipe.hincrby(self.index_info, 'left', 1)
                        pipe.lindex(self.list_name, left)
                        result = pipe.execute()[1]
                        return result
                    else:
                        #finish
                        pipe.multi()
                        self.r.hset(index_info, 'left', 0)
                        self.r.hset(index_info, 'right', self.count())
                        pipe.execute()
                        return None
                    break
                except redis.exceptions.WatchError:
                    continue

    def count(self):
        count = self.r.llen(self.list_name)
        return count

    def left(self):
        left = self.r.hget(self.index_info, 'left')
        return left

    def right(self):
        right = self.r.hget(self.index_info, 'right')
        return right


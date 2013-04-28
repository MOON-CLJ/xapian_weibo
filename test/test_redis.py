import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


@timeit
def test_redis_string_single_write(n):
    for i in range(n):
        r.set(str(i), i)


@timeit
def test_redis_string_read(n):
    for i in range(n):
        r.get(str(i))


@timeit
def test_redis_hash_single_write(n):
    for i in range(n):
        r.hset('test_hash', str(i), i)


@timeit
def test_redis_hash_read(n):
    for i in range(n):
        r.hget('test_hash', str(i))


@timeit
def test_redis_set_single_write(n):
    for i in range(n):
        r.sadd('test_set', str(i))


@timeit
def test_redis_set_pipeline_write(n):
    pipe = r.pipeline()
    for i in range(n):
        pipe.sadd('test_set', str(i))
    pipe.execute()


@timeit
def test_redis_hash_pipeline_write(n):
    pipe = r.pipeline()
    for i in range(n):
        r.hset('test_hash', str(i), i)
    pipe.execute()


def clear(n):
    for i in range(n):
        r.delete(str(i))
    r.delete('test_hash')
    r.delete('test_set')

if __name__ == '__main__':
    n = 100000
    test_redis_string_single_write(n)
    test_redis_string_read(n)

    test_redis_hash_single_write(n)
    r.delete('test_hash')
    test_redis_hash_read(n)

    test_redis_set_single_write(n)

    test_redis_set_pipeline_write(n)
    test_redis_hash_pipeline_write(n)
    clear(n)

    """
    'test_redis_string_single_write' args: (100000,) 5.81 sec
    'test_redis_string_read' args: (100000,) 5.30 sec
    'test_redis_hash_single_write' args: (100000,) 5.99 sec
    'test_redis_hash_read' args: (100000,) 5.64 sec
    'test_redis_set_single_write' args: (100000,) 5.52 sec
    'test_redis_set_pipeline_write' args: (100000,) 1.15 sec
    'test_redis_hash_pipeline_write' args: (100000,) 6.04 sec
    """

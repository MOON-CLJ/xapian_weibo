import time
import leveldb


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


@timeit
def test_leveldb_single_write(n):
    for i in xrange(n):
        db.Put(str(i), str(i))


@timeit
def test_leveldb_single_read(n):
    for i in xrange(n):
        db.Get(str(i))


@timeit
def test_leveldb_multi_write(n):
    batch = leveldb.WriteBatch()
    for i in xrange(n):
        db.Put(str(i), str(i))

    db.Write(batch, sync=True)


if __name__ == '__main__':
    n = 1000000

    db = leveldb.LevelDB('./leveldb')
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

    """
    'test_leveldb_single_write' args: (400000,) 1.22 sec
    'test_leveldb_single_read' args: (400000,) 1.53 sec
    'test_leveldb_multi_write' args: (400000,) 1.22 sec

    'test_leveldb_single_write' args: (1000000,) 2.91 sec
    'test_leveldb_single_read' args: (1000000,) 3.73 sec
    'test_leveldb_multi_write' args: (1000000,) 2.82 sec
    """
    db = leveldb.LevelDB('./leveldb1', block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

    n = 10000000
    db = leveldb.LevelDB('./leveldb2', block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

    db = leveldb.LevelDB('./leveldb3', block_size=256 * 1024, block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

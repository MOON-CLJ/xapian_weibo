#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

    # 初始化推荐使用参数
    db = leveldb.LevelDB('./leveldb1', block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

    """
    'test_leveldb_single_write' args: (1000000,) 18.67 sec
    'test_leveldb_single_read' args: (1000000,) 3.92 sec
    'test_leveldb_multi_write' args: (1000000,) 5.44 sec

    'test_leveldb_single_write' args: (1000000,) 3.42 sec
    'test_leveldb_single_read' args: (1000000,) 2.39 sec
    'test_leveldb_multi_write' args: (1000000,) 5.07 sec
    说明batch写影响不大，但是参数调整对单条写影响很大
    """
    """
    n = 10000000
    db = leveldb.LevelDB('./leveldb2', block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)

    db = leveldb.LevelDB('./leveldb3', block_size=256 * 1024, block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_leveldb_single_write(n)
    test_leveldb_single_read(n)
    test_leveldb_multi_write(n)
    """

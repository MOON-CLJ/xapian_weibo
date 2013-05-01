#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import plyvel


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


@timeit
def test_plyvel_single_write(n):
    for i in xrange(n):
        db.put(str(i), str(i))


@timeit
def test_plyvel_single_read(n):
    for i in xrange(n):
        db.get(str(i))


@timeit
def test_plyvel_multi_write(n):
    with db.write_batch() as wb:
        for i in xrange(n):
            wb.put(str(i), str(i))


if __name__ == '__main__':
    n = 1000000

    db = plyvel.DB('./plyvel', create_if_missing=True)
    test_plyvel_single_write(n)
    test_plyvel_single_read(n)
    test_plyvel_multi_write(n)

    db = plyvel.DB('./plyvel1', create_if_missing=True, lru_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    test_plyvel_single_write(n)
    test_plyvel_single_read(n)
    test_plyvel_multi_write(n)

    db = plyvel.DB('./plyvel2', create_if_missing=True, lru_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25), bloom_filter_bits=10)
    test_plyvel_single_write(n)
    test_plyvel_single_read(n)
    test_plyvel_multi_write(n)
    """
    'test_plyvel_single_write' args: (1000000,) 17.66 sec
    'test_plyvel_single_read' args: (1000000,) 3.92 sec
    'test_plyvel_multi_write' args: (1000000,) 2.06 sec
    'test_plyvel_single_write' args: (1000000,) 3.03 sec
    'test_plyvel_single_read' args: (1000000,) 2.03 sec
    'test_plyvel_multi_write' args: (1000000,) 2.21 sec
    'test_plyvel_single_write' args: (1000000,) 3.03 sec
    'test_plyvel_single_read' args: (1000000,) 2.00 sec
    'test_plyvel_multi_write' args: (1000000,) 2.17 sec
    说明跟pyleveldb一样，调整参数对单条写影响很大
    plyvel性能优于pyleveldb
    plyvel似乎真正起到作用，不像pyleveldb
    """

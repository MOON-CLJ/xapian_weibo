# -*- coding: utf-8 -*-

import time
from pyelevator import WriteBatch, Elevator


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


@timeit
def test_elevator_single_write(n):
    for i in xrange(n):
        db.Put(str(i), str(i))


@timeit
def test_elevator_single_read(n):
    for i in xrange(n):
        db.Get(str(i))


@timeit
def test_elevator_multi_write(n):
    with WriteBatch() as batch:
        for i in xrange(n):
            batch.Put(str(i), str(i))


if __name__ == '__main__':
    n = 100000

    db = Elevator()
    test_elevator_single_write(n)
    test_elevator_single_read(n)
    test_elevator_multi_write(n)

    """
    # tcp
    # elevator -c config/elevator.conf -w 8
    'test_elevator_single_write' args: (100000,) 57.43 sec
    'test_elevator_single_read' args: (100000,) 64.16 sec
    'test_elevator_multi_write' args: (100000,) 0.51 sec

    # tcp
    # elevator -c config/elevator.conf -w 16
    'test_elevator_single_write' args: (100000,) 65.89 sec
    'test_elevator_single_read' args: (100000,) 63.12 sec
    'test_elevator_multi_write' args: (100000,) 0.50 sec
    """

    # ipc
    # elevator -t ipc -c config/elevator.conf -w 8
    """
    not work
    db = Elevator(transport='ipc', endpoint='/tmp/elevator.sock')
    test_elevator_single_write(n)
    test_elevator_single_read(n)
    test_elevator_multi_write(n)
    """

# -*- coding: utf-8 -*-

from riak.client import RiakClient
import time

Nodes = [
    {'host': '219.224.135.60', 'pb_port': 10017, 'http_port': 10018},
    {'host': '219.224.135.60', 'pb_port': 10027, 'http_port': 10028},
    {'host': '219.224.135.60', 'pb_port': 10037, 'http_port': 10038},
    {'host': '219.224.135.60', 'pb_port': 10047, 'http_port': 10048},
    {'host': '219.224.135.60', 'pb_port': 10057, 'http_port': 10058},
]


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


@timeit
def test_riak_write(test_bucket, n):
    for i in range(n):
        test_bucket.new(str(i), data=i).store()


@timeit
def test_riak_read(test_bucket, n):
    for i in range(n):
        r = test_bucket.get(str(i))
        # stable version
        # data = r.get_data()
        # master_version
        data = r.data

        if data != i:
            raise

if __name__ == '__main__':
    # master_version
    client = RiakClient(host='219.224.135.60', pb_port=10017, protocol='pbc')

    # master_version use nodes param
    # 此场景下性能差不多，和上面指定具体port相比
    # client = RiakClient(host='219.224.135.60', nodes=Nodes, protocol='pbc')

    # test
    test_bucket = client.bucket('lijun_test')
    """
    new_node = test_bucket.new('hehe', data='hehe')
    new_node.store()
    r = test_bucket.get('hehe')
    print r.get_data(), type(r.get_data())
    new_node = test_bucket.new('hehe1', data=1)
    new_node.store()
    r = test_bucket.get('hehe1')
    print r.get_data(), type(r.get_data())
    """

    # test performance
    """
    n = 100000
    test_riak_write(test_bucket, n)
    test_riak_read(test_bucket, n)
    """

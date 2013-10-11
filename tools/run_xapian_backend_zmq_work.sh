#!/bin/sh

cd /home/arthas/dev/xapian_weibo/data
for i in `seq 1 10`
do
  echo "$i"
  python /home/arthas/dev/xapian_weibo/xapian_weibo/xapian_backend_zmq_work.py master_timeline_weibo &
done

# -*- coding: utf-8 -*-
import datetime, time, re, os
import leveldb
LEVELDBPATH = '/home/mirage/leveldb'

weibo_repost_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_repost_20131004_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost_20131004'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

from xapian_weibo.xapian_backend import XapianSearch
statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

def getStatusById(mid):
	count, get_results = statuses_search.search(query={'_id': int(mid)}, fields=['_id', 'user'])
	if count:
		for r in get_results():
			mid = r['_id']
			uid = r['user']
			if not mid:
				return None
			return [mid, uid]
	else:
		return None

def getStatusByNameAndRid(name, rid, r_users):
	count, get_results = user_search.search(query={'name': name}, fields=['_id'])
	uid = None
	if count:
		for r in get_results():
			uid = r['_id']
	else:
		return None

	mid = None
	ts = None
	if uid:
		count, get_results = statuses_search.search(query={'user': int(uid), 'retweeted_status': int(rid)}, fields=['_id', 'text', 'timestamp'])
		if count:
			for r in get_results():
				mid = r['_id']
				text = r['text']
				ts = int(r['timestamp'])
				repost_users = re.findall(u'/@([a-zA-Z-_\u0391-\uFFE5]+)', text)
				if repost_users == r_users:
					break
	else:
		return None

	if not mid or not ts:
		return None

	return [mid, uid, ts]

def prepare_from_xapian():
	finish_set = set() #存储已经解析上层转发结构的微博

	total_days = 300
	today = datetime.datetime.today()
	now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
	now_ts = int(now_ts)
	during = 24 * 3600
	begin_ts = now_ts - 260 * during
	now_ts = now_ts - 180 * during

	query_dict = {
	    'timestamp': {'$gt': begin_ts, '$lt': now_ts}
	}

	fields_list = ['text', 'timestamp', 'user', '_id', 'retweeted_status']

	count, get_results = statuses_search.search(query=query_dict, fields=fields_list)
	print 'statuses_count: ', count

	process_count = 0
	for r in get_results():
		if process_count % 10000 == 0:
			print process_count
		process_count += 1

		mid = r['_id']
		uid = r['user']
		ts = int(r['timestamp'])
		if mid and uid and ts:
			reposts_ = [mid, uid, ts]
			if mid not in finish_set:
				finish_set.add(mid)
			else:
				continue

		text = r['text']
		repost_users = re.findall(u'/@([a-zA-Z-_\u0391-\uFFE5]+)', text)
		retweeted_status = r['retweeted_status']
		retweeted_mid = None
		retweeted_uid = None
		if retweeted_status:
			if repost_users and len(repost_users):
				for idx in range(0, len(repost_users)):
					if getStatusByNameAndRid(repost_users[idx], retweeted_status, repost_users[-len(repost_users)+idx+1:-1]):
						repost_mid, repost_uid, repost_ts = getStatusByNameAndRid(repost_users[idx], retweeted_status, repost_users[-len(repost_users)+idx+1:-1])
						if repost_mid not in finish_set:
							finish_set.add(repost_mid)
						else:
							continue
						k = str(reposts_[0])
						v = str(repost_mid) + '_' + str(repost_uid)
						weibo_repost_20131004_bucket.Put(k, v)
						reposts_ = [repost_mid, repost_uid, repost_ts]
			if getStatusById(retweeted_status):
				retweeted_mid, retweeted_uid = getStatusById(retweeted_status)
				k = str(reposts_[0])
				v = str(retweeted_mid) + '_' + str(retweeted_uid)
				weibo_repost_20131004_bucket.Put(k, v)
			else:
				continue
		else:
			continue


def read_repost_data():
	count = 0
	for k, v in weibo_repost_bucket.RangeIter():
		count += 1
	print count

if __name__ == '__main__':
	prepare_from_xapian()
	#read_repost_data()
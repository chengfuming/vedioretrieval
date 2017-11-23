# -*- coding: utf-8 -*-
import sys
import redis
import zlib


if __name__ == '__main__':
    uid = sys.argv[1]
    db1 = redis.Redis('605b6d4597d84518.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY')
    db2 = redis.Redis('c10abe63c32f4733.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY')
    hash_mod = (int(uid) / 10 % 10) % 2
    if hash_mod == 0:
        db = db1
    else:
        db = db2
    value = db.get(uid)
    if not value:
        print '%s not in redis' % uid
        sys.exit(0)
    uncompress_value = zlib.decompress(value)
    print uncompress_value

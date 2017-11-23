# -*- coding: utf-8 -*-
import sys
import redis
import zlib


if __name__ == '__main__':
    uid = sys.argv[1]
    db1 = redis.Redis('rm20623.mars.grid.sina.com.cn', 20623)
    db2 = redis.Redis('rm20624.mars.grid.sina.com.cn', 20624)
    db3 = redis.Redis('rm20625.mars.grid.sina.com.cn', 20625)
    db4 = redis.Redis('rm20626.mars.grid.sina.com.cn', 20626)
    db5 = redis.Redis('rm20627.mars.grid.sina.com.cn', 20627)
    db6 = redis.Redis('rm20628.mars.grid.sina.com.cn', 20628)
    db7 = redis.Redis('rm20629.mars.grid.sina.com.cn', 20629)
    db8 = redis.Redis('rm20630.mars.grid.sina.com.cn', 20630)
    hash_mod = (int(uid) / 10) % 8
    if hash_mod == 0:
        db = db1
    elif hash_mod == 1:
        db = db2
    elif hash_mod == 2:
        db = db3
    elif hash_mod == 3:
        db = db4
    elif hash_mod == 4:
        db = db5
    elif hash_mod == 5:
        db = db6
    elif hash_mod == 6:
        db = db7
    elif hash_mod == 7:
        db = db8
    value = db.get(uid)
    if not value:
        print '%s not in redis' % uid
        sys.exit(0)
    uncompress_value = zlib.decompress(value)
    print uncompress_value

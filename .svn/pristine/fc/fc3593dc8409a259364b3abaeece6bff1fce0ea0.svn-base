# -*- coding: utf-8 -*-
import redis


if __name__ == '__main__':
    db = redis.Redis('a3d53d10fb4e41c1.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY', db=1)

    all_num = 0
    for i in range(0, 4000):
        num = 0
        for j in range(0, 10):
            key = "%s_%s" % (i, j)
            try:
                value = db.get(key)
            except:
                db = redis.Redis('a3d53d10fb4e41c1.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY', db=1)
                value = db.get(key)
            if value:
                num += len(value) / 8
                all_num += len(value) / 8
        print i, num
    print "all mid:%s avg mid:%s" % (all_num, all_num / 4000)

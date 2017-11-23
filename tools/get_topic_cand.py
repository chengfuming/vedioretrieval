# -*- coding: utf-8 -*-
import time
import sys
import redis
import struct


def get_mid_timestamp(mid):
    return (int(mid) >> 22) + 515483463


def unixtime_to_date(unixtime):
    localtime = time.localtime(unixtime)
    return time.strftime("%Y-%m-%d %H:%M:%S", localtime)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage:%s mid' % sys.argv[0]
        sys.exit(1)
    mid = int(sys.argv[1])
    db = redis.Redis('a3d53d10fb4e41c1.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY', db=1)
    keywords_str = db.get(mid)
    if not keywords_str:
        print 'mid no keyword'
        sys.exit(1)
    keywords = keywords_str.split(',')
    for keyword in keywords:
        mids = []
        for i in range(10):
            invert = "%s_%s" % (keyword, i)
            mids_bin = db.get(invert)
            if not mids_bin:
                continue
            for i in range(0, len(mids_bin), 8):
                (mid, ) = struct.unpack('<Q', mids_bin[i: i + 8])
                mids.append(mid)
                print invert, len(mids_bin), mid, unixtime_to_date(get_mid_timestamp(mid))
        # print keyword, mids

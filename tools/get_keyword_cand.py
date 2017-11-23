# -*- coding: utf-8 -*-
import sys
import redis
import struct


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage:%s mid' % sys.argv[0]
        sys.exit(1)
    mid = int(sys.argv[1])
    db = redis.Redis('a3d53d10fb4e41c1.redis.rds.aliyuncs.com', 6379, password='h2U6Xap6nY')
    keywords_str = db.get(mid)
    if not keywords_str:
        print 'mid no keyword'
        sys.exit(1)
    keywords = keywords_str.split(',')
    for keyword in keywords:
        mids = []
        for i in range(5):
            invert = "%s_%s" % (keyword, i)
            mids_bin = db.get(invert)
            if not mids_bin:
                continue
            for i in range(0, len(mids_bin), 8):
                (mid, ) = struct.unpack('<Q', mids_bin[i: i + 8])
                mids.append(mid)
        print keyword, mids

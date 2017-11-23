# -*- coding: utf-8 -*-
import sys
import redis
import struct


if __name__ == '__main__':
    mid1 = sys.argv[1]
    mid2 = sys.argv[2]
    db = redis.Redis('10.77.96.33', 9991)
    keywords1 = db.get(mid1)
    print 'keywords num:', len(keywords1.split(','))
    print keywords1

    keywords2 = db.get(mid2)
    print 'keywords num:', len(keywords2.split(','))
    print keywords2

    inter_set = set(keywords1.split(',')).intersection(set(keywords2.split(',')))
    for keyword in inter_set:
        print 'inter:', keyword

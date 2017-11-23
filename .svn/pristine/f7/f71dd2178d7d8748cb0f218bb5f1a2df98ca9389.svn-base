# -*- coding: utf-8 -*-
import sys
import memcache
import struct


servers = ["10.75.1.115:11222",
           "10.75.6.89:11222",
           "10.75.6.90:11222",
           "10.75.6.91:11222"]


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage:%s uid' % sys.argv[0]
        sys.exit(1)

    uid = int(sys.argv[1])

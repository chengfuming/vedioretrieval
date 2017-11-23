# -*- coding: utf-8 -*-
import urllib
import urllib2
import time
import sys
import os
try:
    import simplejson as json
except Exception, e:
    import json
from HTMLParser import HTMLParser


##------------------TEST START-----------------------------------------
##下列注释内容为测试环境，如果放开注释，则可以单独运行该程序，进行测试
# local_path = os.path.split(os.path.realpath(__file__))[0]
# num = local_path.rfind('/')
# local_path = local_path[0:num]
# sys.path.append(local_path + "/package")
# sys.path.append(local_path + "/lib")
##------------------TEST END ------------------------------------------

# import fastlog


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def fetchUrl(url, timeout=3, try_times=2, need_headers=False):
    beg_time = time.time()
    for i in xrange(try_times):
        try:
            #fastlog.info('call interface: TRY#' + str(i) + " url:" + str(url))
            if need_headers:
                hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                       'Accept-Encoding': 'none',
                       'Accept-Language': 'en-US,en;q=0.8',
                       'Connection': 'keep-alive'}
                req = urllib2.Request(url, headers=hdr)
            else:
                req = urllib2.Request(url)
            f = urllib2.urlopen(req, timeout=timeout)
            result = f.read()

            if result:
                cost_time = (time.time() - beg_time) * 1000
                # fastlog.info('call interface: TRY#' + str(i) + " url:" + str(url) + ' cost: ' + str(cost_time) + ' return: ' + str(len(str(result))))
                return result
            else:
                continue
        except Exception, e:
            # fastlog.error("get " + str(url) + " Exception: " + str(e))
            continue

    # fastlog.error("get " + str(url) + " faild")
    return None


def fetchHttpHeadCode(url, timeout=5, try_times=2, need_headers=False):
    beg_time = time.time()
    for i in xrange(try_times):
        try:
            #fastlog.info('call interface: TRY#' + str(i) + " url:" + str(url))
            if need_headers:
                hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                       'Accept-Encoding': 'none',
                       'Accept-Language': 'en-US,en;q=0.8',
                       'Connection': 'keep-alive'}
                req = urllib2.Request(url, headers=hdr)
            else:
                req = urllib2.Request(url)
            f = urllib2.urlopen(req, timeout=timeout)
            result = f.getcode()

            if result == 200:
                cost_time = (time.time() - beg_time) * 1000
                # fastlog.info('call interface: TRY#' + str(i) + " url:" + str(url) + ' cost: ' + str(cost_time) + ' return: ' + str(len(str(result))))
                return result
            else:
                continue
        except Exception, e:
            # fastlog.error("try_times " + str(i) + " get " + str(url) + " Exception: " + str(e))
            continue

    # fastlog.error("get " + str(url) + " faild")
    return None


def fetchJsonUrl(url, timeout=3, try_times=2, need_headers=False):
    try:
        result = fetchUrl(url, timeout, try_times, need_headers)
        if result:
            result_dict = json.loads(result, "utf-8")
            return result_dict
        else:
            # fastlog.error("get url " + str(url) + " faild")
            return None
    except Exception, e:
        # fastlog.error("get url " + str(url) + " Exception: " + str(e))
        return None


def fetchUrlByPost(url, data, timeout=3, try_times=2, need_headers=False):
    """
    data是个dict
    """
    beg_time = time.time()
    for i in xrange(try_times):
        try:
            #对于参数不是dict形式/不是两层结构的数据，不适用urlencode
            if isinstance(data, dict):
                data = urllib.urlencode(data)

            if need_headers:
                hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                       'Accept-Encoding': 'none',
                       'Accept-Language': 'en-US,en;q=0.8',
                       'Connection': 'keep-alive'}
                req = urllib2.Request(url, data, headers=hdr)
            else:
                req = urllib2.Request(url, data)
            f = urllib2.urlopen(req, timeout=timeout)
            result = f.read()

            if result:
                cost_time = (time.time() - beg_time) * 1000
                #fastlog.info('call interface: ' + str(url) + " data: "+ data +  ' cost: ' + str(cost_time) + ' return: ' + str(result))
                return result
            else:
                continue
        except Exception, e:
            # fastlog.error("get " + str(url) + " Exception: " + str(e))
            return None

    # fastlog.error("get " + str(url) + " faild")
    return None


if __name__ == '__main__':
    url = "http://127.0.0.1:22183/api/multiContentRecommend?maxnum_per_key=5&keyword=%E4%B8%88%E5%A4%AB,nba,http&recom_types=video,weibo,weibohot"
    print "Start"
    print url
    # print(fetchJsonUrl(url, need_headers=True))

    beg_time = time.time()
    fetchJsonUrl(url, need_headers=True)
    cost_time = (time.time() - beg_time) * 1000
    print "End", cost_time

    # url = 'http://i.api.place.weibo.cn/get_poiinfo_by_ids.php?short=1&from=qushi&poiids=1006557885'
    # print "Start"
    # print url
    # print(fetchJsonUrl(url))
    # print(fetchUrl(url))
    # print "End"

    # url = 'http://trend.recom.i.weibo.com/v4/gateway.php'
    # data = {}
    # data['target'] = 'tool'
    # data['method'] = 'setGlobalCache'
    # data['name'] = 'test'
    # data['data'] = 'test_data'
    # print 'Start'
    # print url
    # print(fetchUrlByPost(url, data))
    # print 'End'

    # url = 'http://10.75.14.30:8082/get_fcache_mids?uid=1111681197'
    # result = fetchJsonUrl(url, need_headers=True)
    # num = result['num']
    # mids_info = result['mids_info']
    # ori_day_num = {}
    # for mid_info in mids_info:
    #     ori_day_num[mid_info['ori_day']] = 0
    # for mid_info in mids_info:
    #     ori_day_num[mid_info['ori_day']] += 1
    # print time.strftime("%Y-%m-%d %H:%M:%S"), num, sorted(ori_day_num.iteritems(), key=lambda l: l[0], reverse=True)

    # url = 'http://i.api.weibo.com/2/entity/show.json?source=175965901&object_id=1022:1001603822147326897028'
    # result = fetchJsonUrl(url, need_headers=True)
    # print json.dumps(result, encoding='utf-8')
    # content = result['entity']['content']
    # # print content
    # # print "*" * 50
    # strip_content = strip_tags(content)
    # # print strip_content
    # result['entity']['content'] = strip_content
    # print json.dumps(result, encoding='utf-8')

    # url = "http://r27.domob.cn:8060/wax/win?price=83a774f68bd64710db24ffeb0256e8a677a2384010959e46bff704c2e89f8aaf&e=109&rid=9201212&tr=02-Wo-jD6tnrlM_5YixoGsuo7ZCqcfXid_RR0i4B-5iLp1NJ6np7A4ycFtmhKcd1KeSwbbe-kWQtR8wL7stAG8kRaZbjL3FZ5p3Et2nOYkGfTzNkVAi3tRJTxQWDUA9OY9hWuHt0Jmbe8x372TAiMPi4lODgW9VtJuGxLDN3-OaMvAe8XY40Pi5nc8n0U5t4ZpJCjIpyTvsWQWlLTTm2XbFZMVtFlaNYR6i0liyDeIJ2ounFpXl9zSGZTfAVl9UGMjmhUfrWFOi16Cg152vGKGKiOfa4yhm77nL8pchNqF43kKnbI2DXfBRX8ynmR9DffbKh-Ta5ACZSstPFzVZuLliQRX4nJd2IYRMY7cuzMzqU61UJVoQoCWT38UoYBNAHgmFb9Ec-08A2_uyIipf1YOqn9_3e2XjgsuZ-5eWdJF2ZGSBzkGgPoPkk89quPUMy6nD0r8RIAek7pb72SqlVjORmULXl0rt6WlmgbkilpPWfwcpKoVpGSQlgXGh5iGFXe5j86bz8NUf2OOelK61s4h-fz6XMF9a49iDNKq18wonQNPokOu1X0XAst3EZWoLuVYPVemWS0czMS1lvEGoO5Ay-ePB04LyOAiOer_d_IEqNXcRGJ-aFfbz4-abZnz9ziz0XhD_QfHo6_rFu-4wMTzaA.."

    # print fetchHttpHeadCode(url)
    # uid = 123

    # url = 'http://interface.recom.i.t.sina.com.cn/recom_user/postqueuedata'
    # data = {}
    # data['key'] = 'test'
    # data['server'] = 'mcq.recom.i.weibo.com:21122'
    # info = {}
    # info['User'] = 'dongna'
    # info['JobName'] = 'not_fans'
    # info['ContextType'] = 'shell'
    # info['NotifyType'] = 'mails'
    # contextinfo = {}
    # contextinfo['svn'] = 'https://svn.intra.sina.com.cn/searchengine/labs/recomgroup/person/dongna/spread_uid_v2/'
    # contextinfo['proinfo'] = 'sh loopwork.sh %s not_fans' % uid
    # contextinfo['setting'] = ''
    # info['ContextInfo'] = contextinfo
    # notifyinfo = {}
    # notifyinfo['mails'] = 'dongna'
    # notifyinfo['phones'] = 'dongna'
    # notifyinfo['interfaces'] = ''
    # notifyinfo['type'] = '1'
    # info['NotifyInfo'] = notifyinfo
    # data['info'] = json.dumps(info)
    # print(fetchUrlByPost(url, data))

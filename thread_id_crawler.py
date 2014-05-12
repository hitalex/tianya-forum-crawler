#coding:utf8

"""
抓取天涯论坛的论坛讨论帖ID(Thread ID)
NOTE: 请跟线程Thread自行区分...

Author: kqingchao@gmail.com
Date: 2014.5.12
"""
import sys

from urlparse import urljoin,urlparse
from collections import deque
import traceback
import logging
import time
from datetime import datetime
import pdb
import codecs # for file encodings
import os

from bs4 import BeautifulSoup 
from lxml import etree # use XPath from lxml

from webPage import WebPage
from threadPool import ThreadPool
from patterns import *
#from models import Group
from logconfig import congifLogger

import stacktracer

log = logging.getLogger('Main.ThreadIDCrawler')


class ThreadIDCrawler(object):

    def __init__(self, start_url, thread_num, thread_list_path, max_topics_num = 1000):
        """
        `group_id`          待抓取的group id
        `thread_num`         抓取的线程
        `thread_list_path`   保存所有的thread id list的文件路径
        """
        #线程池,指定线程数
        self.thread_pool = ThreadPool(thread_num)
        # 保存topic的线程
        self.save_thread = ThreadPool(1)

        # 保存group相关信息
        self.thread_list_path = thread_list_path
        
        # 已经访问的页面: Group id ==> True or False
        self.visited_href = set()
        #待访问的小组讨论页面
        self.unvisited_href = deque()
        # 访问失败的页面链接
        self.failed_href = set()
        
        self.start_url = start_url
        
        # 抓取结束有两种可能：1）抓取到的topic数目已经最大；2）已经将所有的topic全部抓取
        # 只保存thread-id
        self.thread_list = list()
        
        self.is_crawling = False
        
        # 每个Group抓取的最大topic个数
        self.MAX_TOPICS_NUM = max_topics_num
        #self.MAX_TOPICS_NUM = float('inf')
        # 每一页中显示的最多的topic数量，似乎每页中不一定显示25个topic
        #self.MAX_TOPICS_PER_PAGE = 25

    def start(self):
        print '\nStart crawling post id list...\n'
        self.is_crawling = True
        self.thread_pool.startThreads()
        self.save_thread.startThreads()
        
        # 打开需要存储的文件
        self.thread_list_file = codecs.open(self.thread_list_path, 'w', 'utf-8')
        
        print "Add start url:", self.start_url
        self.unvisited_href.append(self.start_url)
        
        #分配任务,线程池并发下载当前深度的所有页面（该操作不阻塞）
        self._assignInitTask()
        #等待当前线程池完成所有任务,当池内的所有任务完成时，才进行下一个小组的抓取
        #self.thread_pool.taskJoin()可代替以下操作，可无法Ctrl-C Interupt
        while self.thread_pool.getTaskLeft() > 0:
            #print "Task left: ", self.thread_pool.getTaskLeft()
            # 判断是否已经抓了足够多的thread id
            if len(self.thread_list) > self.MAX_TOPICS_NUM:
                print u'已经达到最大讨论帖抓取数，即将推出抓取。'
                break
            else:
                print u'当前已抓取的讨论帖个数：', len(self.thread_list)
                
            time.sleep(3)

        # 存储抓取的结果并等待存储线程结束
        while self.save_thread.getTaskLeft() > 0:
            print 'Wairting for saving thread. Taks left: %d' % self.save_thread.getTaskLeft()
            time.sleep(3)
        
        log.info("Thread ID list crawling done.")
        
        self.stop()
        # 结束时可能还有任务，但是当前已经抓去了足够量的讨论帖
        #assert(self.thread_pool.getTaskLeft() == 0)
        
        # 关闭文件
        self.thread_list_file.close()
        print "Main Crawling procedure finished!"

    def stop(self):
        self.is_crawling = False
        self.thread_pool.stopThreads()
        self.save_thread.stopThreads()

    def _assignInitTask(self):
        """取出一个线程，并为这个线程分配任务，即抓取网页
        """ 
        while len(self.unvisited_href) > 0:
            # 从未访问的列表中抽出一个任务，并为其分配thread
            url = self.unvisited_href.popleft()
            self.thread_pool.putTask(self._taskHandler, url)
            # 添加已经访问过的小组id
            self.visited_href.add(url)
            
    def _taskHandler(self, url):
        """ 根据指定的url，抓取网页，并进行相应的访问控制
        """
        print "Visiting : " + url
        webPage = WebPage(url)
        # 抓取页面内容
        flag = webPage.fetch()
        if flag:
            url, pageSource = webPage.getDatas()
            hrefs = self._getAllHrefsFromPage(url, pageSource)
            # 找到有效的链接
            thread_list = []
            next_page_url = None
            for href in hrefs:
                # 只有满足讨论帖链接格式的链接才会被处理
                m = regex_thread.match(href)
                if self._isHttpOrHttpsProtocol(href) and m is not None:
                    thread_list.append(m.group(1))

                # 在当前页面中查找匹配“下一页”的链接
                m = regex_next_page.match(href)
                if m != None and (not m.group() in self.visited_href):
                    url = m.group()
                    print 'Add next page link: ', url
                    self.thread_pool.putTask(self._taskHandler, url)
                    self.visited_href.add(url)
                                
            for thread in thread_list:
                #print "Add thread link: ", thread
                self.thread_list.append(thread)
                
            # 存储已经抓取的topic list
            self.save_thread.putTask(self._saveTopicHandler, thread_list)            
        else:                
            log.error(u"抓取讨论帖列表时，发现网址格式错误。URL: %s" % url)
            # if page reading fails
            self.failed_href.add(url)
            return False
            
    def _saveTopicHandler(self, thread_list):
        """ 将每次从页面中抓取的topic id随时保存到文件中
        NOTE: saveThread只有一个，所以这里不会造成访问冲突
        """
        for tid in thread_list:
            self.thread_list_file.write(tid + '\n')
            
        self.thread_list_file.flush()
        os.fsync(self.thread_list_file)

    def _getAllHrefsFromPage(self, url, pageSource):
        '''解析html源码，获取页面所有链接。返回链接列表'''
        hrefs = []
        soup = BeautifulSoup(pageSource)
        results = soup.find_all('a',href=True)
        for a in results:
            #必须将链接encode为utf8, 因为中文文件链接如 http://aa.com/文件.pdf 
            #在bs4中不会被自动url编码，从而导致encodeException
            href = a.get('href').encode('utf8')
            if not href.startswith('http'):
                href = urljoin(url, href)#处理相对链接的问题
            hrefs.append(href)
        return hrefs

    def _isHttpOrHttpsProtocol(self, href):
        protocal = urlparse(href).scheme
        if protocal == 'http' or protocal == 'https':
            return True
        return False
        
    def _getAlreadyVisitedNum(self):
        #visitedGroups保存已经分配给taskQueue的链接，有可能链接还在处理中。
        #因此真实的已访问链接数为visitedGroups数减去待访问的链接数
        if len(self.visited_href) == 0:
            return 0
        else:
            return len(self.visited_href) - self.thread_pool.getTaskLeft()
        
if __name__ == "__main__":
    stacktracer.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
    congifLogger("log/thread-id-crawler.log", 5)
    
    # 从这个URL开始抓取，从这个页面抓取下一页的地址，只能是单各进程
    start_url = 'http://bbs.tianya.cn/list-free-1.shtml'        
    print "Start URL:", start_url
    
    base_path = '/home/kqc/dataset/tianya-forum/'
    time_now = datetime.now()    
    thread_id_list_path = 'thread-id-list.txt'
    
    tcrawler = ThreadIDCrawler(start_url, 1, thread_id_list_path, max_topics_num = 500)
    tcrawler.start()
    
    stacktracer.trace_stop()


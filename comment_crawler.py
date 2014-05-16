#coding:utf8

"""
根据已经抓取到的每个小组的topic列表，针对具体的每个topic抓取评论
主题框架于topic_crawler.py相似
"""

from urlparse import urljoin,urlparse
from collections import deque
import traceback
import logging
import time
from datetime import datetime
import pdb
import codecs # for file encodings
import os
import sys

from bs4 import BeautifulSoup 

from webPage import WebPage
from threadPool import ThreadPool
from patterns import *
from models import Post
from logconfig import congifLogger

import stacktracer

log = logging.getLogger('Main.CommentCrawler')


class CommentCrawler(object):
    
    def __init__(self, section_id, post_id_list, crawler_thread_num, save_thread_num, post_base_path):
        """
        `section_id` 天涯的板块名称
        `post_id_list` 需要抓取的post id的list
        `thread_num` 开启的线程数目
        post_base_path: 存储抓取结果的基本目录，每个post一个文件，并以该post的ID命名
        """
        # 抓取网页的线程池,指定线程数
        self.thread_pool = ThreadPool(crawler_thread_num)
        # 由于现在是将不同的topic信息保存到不同的文件中，所以可以同时存储
        self.save_thread = ThreadPool(save_thread_num)
        
        # 保存抓取信息的base path
        self.base_path = post_base_path
        
        # 已经访问的页面: Group id ==> True or False
        self.visited_href = set()
        self.visited_post = set() # 已经添加访问的页面的id集合
        self.finished = set() # 已经抓取完毕的topic id集合
        
        # 抓取失败的topic id
        self.failed = set()
        
        # 依次为每个小组抽取topic评论
        self.section_id = section_id
        self.post_id_list = post_id_list # 等待抓取的topic列表
        self.current_post_id_list = list(post_id_list) # 用于逐步向任务列表中加入post id
        
        # 存储结果
        # topic ID ==> Topic对象
        self.post_dict = dict()
        # 存放下一个处理的评论页数： topic ID ==> 1,2,3...
        self.next_page = dict()

        self.is_crawling = False
        
        # 每个topic抓取的最多comments个数
        #self.MAX_COMMETS_NUM = 1000
        self.MAX_COMMETS_NUM = float('inf')

    def start(self):
        print '\nStart Crawling comment list for group: ' + self.section_id + '...\n'
        self.is_crawling = True
        self.thread_pool.startThreads()
        self.save_thread.startThreads()
        
        self.post_id_list = list(set(self.post_id_list)) # 消除重复的topic id
        print u"Total number of post in section %s: %d." % (self.section_id, len(self.post_id_list))
        
        # 先为字典建立所有的key，避免出现“RuntimeError: dictionary changed size during iteration”错误
        for post_id in self.post_id_list:
            self.post_dict[post_id] = None
        
        # 初始化添加一部分post的id到列表
        for i in xrange(self.thread_pool.threadNum * 2):
            # TODO: 这里的URL模式只是针对“天涯杂谈”部分的链接
            if len(self.current_post_id_list) > 0:
                post_id = self.current_post_id_list.pop()
                url = "http://bbs.tianya.cn/post-%s-%s-1.shtml" % (self.section_id, post_id)
                self.thread_pool.putTask(self._taskHandler, url)
        
        # 完全抛弃之前的抽取深度的概念，改为随时向thread pool推送任务
        while True:
            # 保证任何时候thread pool中的任务数最少为线程数的2倍
            print "Check threalPool queue..."
            while self.thread_pool.getTaskLeft() < self.thread_pool.threadNum * 2:
                # 获取未来需要访问的链接
                url = self._getFutureVisit()
                if url is not None: 
                    self.thread_pool.putTask(self._taskHandler, url)
                else: # 已经不存在下一个链接
                    #print 'No future visit url.'
                    break
            # 每隔一秒检查thread pool的队列
            time.sleep(2)
            # 检查是否处理完毕
            if len(self.finished) == len(self.post_id_list):
                break
            elif len(self.finished) > len(self.post_id_list):
                assert(False)
                
            print 'Number of task in LIFO queue: ', self.thread_pool.taskQueue.qsize()
            print 'Total posts: %d, Finished topic: %d' % (len(self.post_id_list), len(self.finished))
                
        # 等待线程池中所有的任务都完成
        print "Totally visited: ", len(self.visited_href)
        #pdb.set_trace()
        while self.thread_pool.getTaskLeft() > 0:
            print "Task left in threadPool: ", self.thread_pool.getTaskLeft()
            print "Task queue size: ", self.thread_pool.taskQueue.qsize()
            print "Running tasks: ", self.thread_pool.running
            time.sleep(2)
        
        # 检查保存线程完成情况
        while self.save_thread.getTaskLeft() > 0:
            print "Task left in save thread: ", self.save_thread.getTaskLeft()
            print "Task queue size: ", self.save_thread.taskQueue.qsize()
            print "Running tasks: ", self.save_thread.running
            time.sleep(2)
        
        # 记录抓取失败的topic id
        log.info(u'抓取失败的post id：')
        s = ''
        for post_id in self.failed:
            s += (post_id + '\n')
        log.info('\n' + s)
        
        print "Terminating all threads..."
        self.stop()
        assert(self.thread_pool.getTaskLeft() == 0)
        
        print "Main Crawling procedure finished!"
        log.info("Processing done with tianya section: %s" % (self.section_id))

    def stop(self):
        self.is_crawling = False
        self.thread_pool.stopThreads()
        self.save_thread.stopThreads()
        
    def _taskHandler(self, url):
        """ 根据指定的url，抓取网页，并进行相应的访问控制
        """      
        print "Visiting : " + url
        webPage = WebPage(url)
        
        # 抓取页面内容
        flag = webPage.fetch()
        m = regex_post.match(url)
        if m == None:
            log.info(u'Post链接格式错误：%s in Group: %s.' % (url, self.section_id))
            return True
        else:
            log.info(u'访问：' + url)
            
        comment_page_index = int(m.group('page_index'))
        post_id = m.group('post_id')
        if flag:
            if comment_page_index == 1: # 首页评论
                post = Post(post_id, self.section_id)
                # 解析讨论帖的第一个页：包括原帖内容和评论内容
                comment_list = post.parse(webPage, isFirstPage = True) # First page parsing
                self.post_dict[post_id] = post
                self.next_page[post_id] = 2
                
            elif comment_page_index > 1:
                # 抽取非第一页的评论数据
                if post_id in self.post_dict:
                    post = self.post_dict[post_id]
                else:
                    # 这里的含义为：必须先处理第一页的评论，否则该post_id不会作为self.topic_dict的键出现
                    log.error(u'错误：必须先抽取第一页的评论数据：post id: %s' % post_id)
                    self.failed.add(topic_id)
                    self.finished.add(topic_id)
                    return False
                
                if post is None:
                    log.error(u'未知程序错误：结束post id为%s的抽取，释放内存。' % post_id)
                    self.post_dict[post_id] = post
                    return False
                    
                comment_list = post.parse(webPage, isFirstPage = False) # non-firstpage parsing
            else:
                log.info(u'Post链接格式错误：%s in Group: %s.' % (url, self.section_id))

            # 判断抓取是否结束，如果结束，则释放dict内存
            # 这个很重要，因为随着topic数量增多，内存会占很多
            if post.is_complete():
                self.save_thread.putTask(self._saveTopicHandler, self.post_dict, post_id)
                self.finished.add(post_id)
                log.info(u'Topic: %s 抓取结束。' % post_id)
                
            self.visited_href.add(url)
            return True
        else:
            # 处理抓取失败的网页集合，只要一个网页抓取失败，则加入到finished
            # 添加抓取失败的post id和标记抓取结束的post
            self.failed.add(post_id)
            self.finished.add(post_id) # 有可能已经记录了一些某些topic的信息
            self.visited_href.add(url)
            return False

    def _getFutureVisit(self):
        """根据当前的访问情况，获取下一个要访问的网页
        """
        # 先检查当前正在抓取的所有帖子，目标是尽快将其抓去完并保存
        for post_id in self.post_dict:
            if post_id in self.finished:
                continue
            post = self.post_dict[post_id]
            
            if post is None:
                continue
                
            if post.total_comment_page <= 0:
                # 还未处理该topic的首页
                continue
            elif post.total_comment_page == 1:
                # 该topic只有首页有评论
                continue
            else:
                # 该topic有多页评论
                next_page_index = self.next_page[post_id]
                if next_page_index > post.total_comment_page:
                    continue
                else:
                    url = "http://bbs.tianya.cn/post-free-%s-%d.shtml" % (post_id, next_page_index)
                    self.next_page[post_id] = next_page_index + 1
                    return url
                
        # 如果当前正在处理的帖子全部已经抓取完毕，则加入新帖子post_id
        if len(self.current_post_id_list) > 0:
            post_id = self.current_post_id_list.pop()
            url = "http://bbs.tianya.cn/post-%s-%s-1.shtml" % (self.section_id, post_id)
            return url
        else:
            return None
    
    def _saveTopicHandler(self, post_dict, post_id):
        """ 存储抓取完毕的帖子信息以及其对应的Comment。
        不过，跟_saveHandler函数不同的是，这里是按照topic id存储
        post_dict 存储topic信息的字典
        post_id 需要存储的post id
        """
        # 在保存结果钱，对评论进行排序，并查找quote comment        
        post = post_dict[post_id]
        post.sort_comment()
        
        post_path = self.base_path + self.section_id + '/' + post_id + '-info.txt'
        # 存储topic本身的信息
        f = codecs.open(post_path, 'w', 'utf-8')
        s = post.get_simple_string('[=]')
        f.write(s + '\n')
        
        # 存储comment信息,存储到相同的文件中
        for comment in post.comment_list:
            s = comment.get_simple_string('[=]')
            f.write(s + '\n')
        f.close()
        
        # 释放资源
        # NOTE: del self.post_dict[post_id]不能达到效果，如果需要根据post_id是否在
        # self.post_dict中来判断是否已经抓取该帖子
        self.post_dict[post_id] = None
        self.next_page[post_id] = None
        
        log.info(u"Topic: %s 存储结束。" % post_id)

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
        
if __name__ == "__main__":
    LINE_FEED = "\n" # 采用windows的换行格式
    stacktracer.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
    congifLogger("log/comment_crawler.log", 5)
    
    section_id = 'free'
    post_base_path = '/home/kqc/dataset/tianya-forum/'
    
    import sys
    post_list_path = sys.argv[1]

    import os
    #post_list_path = post_base_path + ('%s-test-list.txt' % section_id)
    f = codecs.open(post_list_path, 'r', 'utf8')
    post_id_list = []
    for line in f:
        line = line.strip()
        if line == "":
            continue
        # 如果已经抓取，同样放弃
        post_id = line
        path = post_base_path + section_id + '/' + post_id + '-info.txt'
        if os.path.exists(path):
            continue
            
        post_id_list.append(line)
    f.close()
        
    comment_crawler = CommentCrawler(section_id, post_id_list, 5, 10, post_base_path)
    comment_crawler.start()
    
    print "Done"
    stacktracer.trace_stop()

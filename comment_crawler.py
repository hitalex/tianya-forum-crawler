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
#from models import Topic
from logconfig import congifLogger

import stacktracer

log = logging.getLogger('Main.CommentCrawler')


class CommentCrawler(object):
    
    def __init__(self, section_name, post_id_list, crawler_thread_num, save_thread_num, post_base_path):
        """
        `section_name` 天涯的板块名称
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
        # 抓取失败的topic id
        self.failed = set()
        
        # 依次为每个小组抽取topic评论
        self.section_name = section_name
        self.post_id_list = post_id_list # 等待抓取的topic列表
        
        # 存储结果
        # topic ID ==> Topic对象
        self.post_dict = dict()
        # 存放下一个处理的评论页数： topic ID ==> 1,2,3...
        self.next_page = dict()
        # 已经抓取完毕的topic id集合
        self.finished = set()

        self.is_crawling = False
        
        # 每个topic抓取的最多comments个数
        #self.MAX_COMMETS_NUM = 5000
        self.MAX_COMMETS_NUM = float('inf')
        

    def start(self):
        print '\nStart Crawling comment list for group: ' + self.section_name + '...\n'
        self.is_crawling = True
        self.thread_pool.startThreads()
        self.save_thread.startThreads()
        
        self.post_id_list = list(set(self.post_id_list)) # 消除重复的topic id
        print u"Total number of post in section %s: %d." % (self.section_name, len(self.post_id_list))
        
        # 初始化添加任务
        for post_id in self.post_id_list:
            # TODO: 这里的URL模式只是针对“天涯杂谈”部分的链接
            url = "http://bbs.tianya.cn/post-free-%s-1.shtml" % post_id
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
                    break
            # 每隔一秒检查thread pool的队列
            time.sleep(2)
            # 检查是否处理完毕
            if len(self.finished) == len(self.post_id_list):
                break
            elif len(self.finished) > len(self.post_id_list):
                assert(False)
                
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
        log.info('抓取失败的post id：')
        s = ''
        for post_id in self.failed:
            s += (post_id + '\n')
        log.info('\n' + s)
        
        print "Terminating all threads..."
        self.stop()
        assert(self.thread_pool.getTaskLeft() == 0)
        
        print "Main Crawling procedure finished!"
        log.info("Processing done with tianya section: %s" % (self.section_name))

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
        match_obj = RETopic.match(url)
        match_obj2 = REComment.match(url)
        
        if flag:
            if match_obj is not None:
                topic_id = match_obj.group(1)
                topic = Topic(topic_id, self.section_name)
                comment_list = topic.parse(webPage, isFirstPage = True) # First page parsing
                self.topic_dict[topic_id] = topic
            elif match_obj2 is not None:
                topic_id = match_obj2.group(1)
                start = int(match_obj2.group(2))
                # 抽取非第一页的评论数据
                if topic_id in self.topic_dict:
                    topic = self.topic_dict[topic_id]
                    if topic is None:
                        log.error('未知程序错误：结束topic id为%s的抽取，释放内存。' % topic_id)
                        self.topic_dict[topic_id] = None
                        return False
                else:
                    # 这里的含义为：必须先处理第一页的评论，否则该topic_id不会作为self.topic_dict的键出现
                    log.error('错误：必须先抽取第一页的评论数据：topic id: %s' % topic_id)
                    self.failed.add(topic_id)
                    self.finished.add(topic_id)
                    return False
                    
                comment_list = topic.parse(webPage, isFirstPage = False) # non-firstpage parsing
                # 保存到单个文件（已废弃不用）
                #self.save_thread.putTask(self._saveHandler, comment_list, topic = None)
            else:
                #pdb.set_trace()
                log.info('Topic链接格式错误：%s in Group: %s.' % (url, self.section_name))
            # 判断抓取是否结束，如果结束，则释放dict内存
            # 这个很重要，因为随着topic数量增多，内存会占很多
            if topic.isComplete():
                # 对评论进行排序，并查找quote comment
                self.topic_dict[topic_id].sortComment()
                self.save_thread.putTask(self._saveTopicHandler, self.topic_dict, topic_id)
                #self.topic_dict[topic_id] = None        # 释放资源
                self.finished.add(topic_id)
                log.info('Topic: %s 抓取结束。' % topic_id)
                
            self.visited_href.add(url)
            return True
        else:
            # 处理抓取失败的网页集合
            # 只要一个网页抓取失败，则加入到finished
            if match_obj is not None:
                # 讨论贴的第一页就没有抓到，则将其列入finished名单中
                topic_id = match_obj.group(1)
            elif match_obj2 is not None:
                topic_id = match_obj2.group(1)
                start = int(match_obj2.group(2))
            else:
                log.info('Topic链接格式错误：%s in Group: %s.' % (url, self.section_name))
            
            # 添加抓取失败的topic id和标记抓取结束的topic
            self.failed.add(topic_id)
            self.finished.add(topic_id) # 有可能已经记录了一些某些topic的信息
            self.visited_href.add(url)
            return False

    def _getFutureVisit(self):
        """根据当前的访问情况，获取下一个要访问的网页
        """
        for topic_id in self.topic_dict:
            if topic_id in self.finished:
                continue
            topic = self.topic_dict[topic_id]
            if topic is None:
                continue
            if topic.max_comment_page <= 0:
                # 还未处理该topic的首页
                continue
            elif topic.max_comment_page == 1:
                # 该topic只有首页有评论
                continue
            else:
                # 该topic有多页评论
                next_start = self.next_page[topic_id]
                url = "http://www.douban.com/group/topic/" + topic_id + "/?start=" + str(next_start * self.COMMENTS_PER_PAGE)
                if next_start <= topic.max_comment_page-1:
                    self.next_page[topic_id] = next_start + 1
                    return url
                else:
                    continue
        
        return None
    
    def _saveTopicHandler(self, topic_dict, topic_id):
        """ 存储抓取完毕的帖子信息以及其对应的Comment。
        不过，跟_saveHandler函数不同的是，这里是按照topic id存储
        @topic_dict 存储topic信息的字典
        @topic_id 需要存储的topic id
        """
        topic = topic_dict[topic_id]
        topic_path = self.base_path + group_id + '/' + topic_id + '-info.txt'
        # 存储topic本身的信息
        f = codecs.open(topic_path, 'w', 'utf-8')
        s = topic.getSimpleString('[=]')
        f.write(s + '\n')
        #f.write('[*ROWEND*]')
        
        # 存储comment信息,存储到相同的文件中
        for comment in topic.comment_list:
            s = comment.getSimpleString('[=]')
            #f.write(s + '\n[*ROWEND*]\n')
            f.write(s + '\n')
        f.close()
        
        self.topic_dict[topic_id] = None        # 释放资源
        log.info("Topic: %s 存储结束" % topic_id)   

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
    LINE_FEED = "\n" # 采用windows的换行格式
    stacktracer.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
    congifLogger("log/comment_crawler.log", 5)
    
    import sys
    post_list_path = sys.argv[1]

    MAX_TOPIC_NUM = float('inf') # 每个小组最多处理的topic的个数
    f = codecs.open(post_list_path, 'r', 'utf8')
    post_id_list = []
    for line in f:
        line = line.strip()
        if line is not "":
            post_id_list.append(line)
    f.close()
    
    section_name = u'天涯杂谈'
    post_base_path = '/home/kqc/dataset/tianya-forum/' + section_name + '/'
    
    comment_crawler = CommentCrawler(section_name, post_id_list, 5, 10, post_base_path)
    comment_crawler.start()
    
    print "Done"
    stacktracer.trace_stop()

# -*- coding: utf-8 -*-

#! /usr/bin/env python
import sys

from datetime import datetime
import logging
from lxml import etree # use XPath from lxml
import operator
import re

# for debug
import pdb
from threading import Lock

from patterns import *
from logconfig import congifLogger

log = logging.getLogger('Main.models')

"""
在这里，我将定义会用到的类和数据结构，包括讨论帖和评论。它们之间的关系为：
一个小组包括一些topic，每个评论包括一些评论
注意：
所有的文本全部用UTF-8来编码

NOTE: 关于用户的链接页面，
如果给定用户ID：http://www.tianya.cn/90086386
如果给定用户昵称：http://www.tianya.cn/n/小C厨娘
"""

class Comment(object):
    """评论的类
    """
    def __init__(self, cid, user_id, user_name, pubdate, content, quote, post_id, section_id):
        self.cid = cid              # 评论id
        self.user_id = user_id      # 发评论的人的id
        self.user_name = user_name  # 发评论人的用户名
        self.pubdate = pubdate      # 发布时间
        self.content = content      # 评论内容，不包括引用评论的内容
        
        self.quote = quote          # 引用他人评论, Comment 类
        
        self.post_id = post_id              # 所在post的id
        self.section_id = section_id    # 所在板块的名称
        
    def __repr__(self):
        # 默认的换行采用Unix/Linux方式
        if not ('LINE_FEED' in dir()):
            LINE_FEED = u"\n"
        s = u"评论id：" + self.cid + LINE_FEED
        s += u"评论人id：" + self.user_id + LINE_FEED
        s += u"评论人用户名：" + self.user_name + LINE_FEED
        s += u"发表时间：" + str(self.pubdate) + LINE_FEED
        if self.quote is not None:
            s += u"引用评论的id：" + self.quote.cid + LINE_FEED
            s += u'引用评论人：' + self.quote.user_name + LINE_FEED
        s += u"内容：" + LINE_FEED + self.content + LINE_FEED
        
        return s
        
    def get_simple_string(self, delimiter):
        """ 获取简单字符串表示
        """
        s = u''
        s += (self.cid + delimiter)
        s += (self.section_id + delimiter)
        s += (self.post_id + delimiter)
        s += (self.user_id + delimiter)
        s += (self.user_name + delimiter)
        s += (str(self.pubdate) + delimiter)

        if self.quote is None:
            s += delimiter
        else:
            s += (self.quote.cid + delimiter)
            
        # self.content中的换行符去掉
        self.content = self.content.replace('\r', ' ')
        self.content = self.content.replace('\n', ' ')
        
        s += (self.content)
        
        return s
        
class Post(object):
    """小组中的某个话题
    """
    def __init__(self, post_id, section_id):
        self.post_id = post_id             # 帖子的ID
        self.section_id = section_id    # 所在的板块
        
        self.user_id = ""           # 发布topic的人的id
        self.user_name = ""         # 用户的昵称
        self.pubdate = ""           # 该topic发布的时间
        self.title = ""             # 该topic的标题
        self.content = ""           # topic的内容
        
        # 在多线程环境下，可能有多个线程同时修改一个Topic的评论列表
        self.lock = Lock()
        self.comment_list = []      # 所有评论的列表, 属于Comment类
        
        self.total_comment_page = 0       # 这个topic具有多少页的评论(包括帖子的首页), init with 0
        # 已经抓取的评论的页面的index
        self.parsedPageIndexSet = set()
        
    def __repr__(self):
        if not ('LINE_FEED' in dir()):
            LINE_FEED = u"\n"
        s = u"话题 id: " + self.post_id + LINE_FEED
        s += u"小组 id: " + self.section_id + LINE_FEED
        s += u"楼主 id: " + self.user_id + u" 名号: " + self.user_name + LINE_FEED
        s += u"发表时间: " + str(self.pubdate) + LINE_FEED
        s += u"链接：" + self.get_self_link() + LINE_FEED
        s += u"标题: " + self.title + LINE_FEED
        #s += u"Max number of comment page: " + str(self.total_comment_page) + LINE_FEED
        s += u"帖子内容: " + LINE_FEED + self.content + LINE_FEED + LINE_FEED
        
        # 添加评论内容
        if len(self.comment_list) == 0:
            s += u"（无评论）" + LINE_FEED
        else:
            s += u"评论内容：" + LINE_FEED
            for comment in self.comment_list:
                s += (comment.__repr__() + "\n")
        
        return s
        
        
    def get_simple_string(self, delimiter):
        """ 获取简单字符串表示，不过不包括comment
        """
        s = u""
        s += (self.post_id + delimiter)
        s += (self.section_id + delimiter)
        s += (self.user_id + delimiter)
        s += (self.user_name + delimiter)
        # 将title中的换行符去掉
        self.title = self.title.replace('\r', ' ')
        self.title = self.title.replace('\n', ' ')
        s += (self.title + delimiter)
        
        s += (str(self.pubdate) + delimiter)
        s += (str(len(self.comment_list)) + delimiter) # number of comments
        # 记录最后一个comment的时间
        #last_comment = self.comment_list[-1]
        #s += (str(last_comment.pubdate) + delimiter)
        
        # 以后可能还需要记录推荐数和喜欢数等
        
        # self.content中的换行符去掉
        self.content = self.content.replace('\r', ' ')
        self.content = self.content.replace('\n', ' ')
        
        s += self.content
        
        return s
        
    def get_self_link(self):
        """ 获取自身的链接
        """
        url = "http://bbs.tianya.cn/post-%s-%s-1.shtml" % (self.section_id, self.post_id)
        return url
        
    def parse(self, webPage, isFirstPage):
        """ 从网页中抽取信息，填写类中的字段
        @webPage 网页数据
        @isFirstPage 是否是topic评论的第一页
        返回新添加的comment list
        """
        if isFirstPage:
            return self.extract_first_page(webPage)
        else:
            return self.extract_nonfirst_page(webPage)
            
    def is_complete(self):
        """ 判断评论抓取是否结束
        """
        if self.total_comment_page == 0:
            return False
        
        if len(self.parsedPageIndexSet) < self.total_comment_page:
            return False
        else:
            return True
            
    def extract_content(self, cnode):
        """ 抽取帖子内容或评论的所有内容
        """
        # 此句只抽取第一个sub element之前的文本
        content = cnode.text.strip()
        # 抓取其他的内容
        for kid in cnode.iterchildren():
            if kid.tag == 'a': # 评论回复
                if kid.text != None:
                    content += kid.text + ' '
                if kid.tail != None:
                    content += (kid.tail + ' ')
                content += '\t'
            elif kid.tag == 'br' and kid.tail != None:
                content += (kid.tail.strip() + '\t')
            elif kid.tag == 'img':
                content += u"（图片：" + kid.attrib['src'] + u"）" + "\t"
            else:
                pass
                
        return content
        
    def extract_first_page(self, webPage):
        """ 抽取topic首页的topic内容和评论
        返回新添加的comment list
        """
        # 抽取topic首页的内容
        url = "http://bbs.tianya.cn/post-%s-%s-1.shtml" % (self.section_id, self.post_id)
        log.info("Extracting first page: " + url)
        
        # for debug
        if isinstance(webPage, unicode):
            pageSource = webPage
        else:
            url, pageSource = webPage.getDatas() # pageSource已经为unicode格式
            
        page = etree.HTML(pageSource)
        post_head = page.xpath(u"//div[@id='post_head']")[0]
        # 找到标题：如果标题太长，那么标题会被截断，原标题则会在帖子内容中显示
        # 如果标题不被截断，则原标题不会在帖子内容中显示
        
        tmp = post_head.xpath(u"h1[@class='atl-title']/span[@class='s_title']")[0]
        iter_obj = tmp.itertext()
        try:
            while True:
                self.title += (iter_obj.next() + ' ')
        except StopIteration:
            pass
        
        atl_info = post_head.xpath(u"div/div[@class='atl-info']/span")
        #assert(len(atl_info) == 4)
        for span in atl_info:
            if span.text == None:
                continue
                
            if u'楼主' in span.text:
                # 作者信息
                anode = atl_info[0].xpath(u"a")[0]
                self.user_id = anode.attrib['uid']
                self.user_name = anode.attrib['uname']
            elif u'时间' in span.text:
                # 发表日期，确保其实unicode编码
                text = atl_info[1].text.strip()
                self.pubdate = datetime.strptime(text[3:], "%Y-%m-%d %H:%M:%S")
        
        # 设置本帖子的最大评论页数
        paginator = post_head.xpath(u"//div[@class='atl-pages']//a")
        # NOTE: paginator会抽取页面上所有的页面链接..
        if len(paginator) == 0:
            self.total_comment_page = 1 # 如果没有paginator，则只有一页评论
        else:
            last_page_url = paginator[-2].attrib['href'].strip() # 最后一页评论的链接
            m = regex_post.search(last_page_url)
            if m == None:
                log.info('Bad url: ' + last_page_url )
            max_page_text = m.group('page_index')
            self.total_comment_page = int(max_page_text)
        print "Total comment page: %d" % self.total_comment_page
        
        # 抽取帖子内容和评论信息
        content_comment_list = page.xpath(u"//div[@class='clearfix']/div[@class='atl-main']/div[@class='atl-item']")
        # 获取帖子的内容
        content_node = content_comment_list[0].xpath(u"div[@class='atl-content']/div[@class='atl-con-bd clearfix']/div[@class='bbs-content clearfix']")[0]
        self.content = self.extract_content(content_node)
        
        # Note: 有可能一个topic下没有评论信息
        newly_added = [] # 本页中新添加的comment
        # 只处理有评论的情形
        if len(content_comment_list) > 1:
            for comment_node in content_comment_list[1:]:
                comment = self.extract_comment(comment_node)
                # 为commen_list加锁
                #pdb.set_trace()
                if comment is None:
                    continue
                self.lock.acquire()
                self.comment_list.append(comment)
                newly_added.append(comment)
                self.lock.release()
            
        # 添加已经抓取的page index
        self.parsedPageIndexSet.add(1)        
        return newly_added
        
    def extract_comment(self, comment_node):
        """ 给定评论节点，抽取评论用户/时间/内容
        """
        # 页面会返回该评论的index，但是或许不可用，因为可能包含删除的评论
        cid = comment_node.attrib['id']

        # 发表时间
        text = comment_node.attrib['js_restime']
        pubdate = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        
        # 从comment节点中抽取出Comment结构，并返回Comment对象
        head_node = comment_node.xpath(u"div[@class='atl-head']")[0]
        #cid = head_node.attrib['id'] # 不再使用这个毫无意义的字符串，改用天涯提供的id
        
        span_list = head_node.xpath(u"div[@class='atl-info']/span")
        info_node = span_list[0]        
        anode = info_node.xpath(u"a")[0]
        user_name = anode.attrib['uname']
        user_id = anode.attrib['uid']
        
        content_node = comment_node.xpath(u"div[@class='atl-content']/div[@class='atl-con-bd clearfix']/div[@class='bbs-content']")[0]
        comment_content = self.extract_content(content_node)
        
        #print 'Lou id: ', cid
        #print 'Comment content: \n', comment_content
        #print ''
        
        # 这里暂不设置comment所引用的quote，而是只是设立标志位has_quote, 具体quote在抓取完topic之后再确定
        comment = Comment(cid, user_id, user_name, pubdate, comment_content, None, self.post_id, self.section_id)
        #print "Comment content: ", comment.content
        return comment
        
    def extract_nonfirst_page(self, webPage):
        """ 抽取非第一页的评论
        """
        if isinstance(webPage, unicode):
            pageSource = webPage
        else:
            url, pageSource = webPage.getDatas() # pageSource已经为unicode格式

        page = etree.HTML(pageSource)
        content_comment_list = page.xpath(u"//div[@class='clearfix']/div[@class='atl-main']/div[@class='atl-item']")
        # Note: 有可能一个topic下没有评论信息
        newly_added = [] # 本页中新添加的comment
        for cnode in content_comment_list:
            comment = self.extract_comment(cnode)
            # 为commen_list加锁
            self.lock.acquire()
            self.comment_list.append(comment)
            newly_added.append(comment)
            self.lock.release()
        
        # 实际抓取网页时用
        m = regex_post.match(url)
        page_index = int(m.group('page_index'))
        self.parsedPageIndexSet.add(page_index)
        
        return newly_added
        
    def find_previous_comment(self, end_index, quote_uname, quote_date):
        """ 根据引用用户昵称和评论时间，找到引用的评论的链接
        """
        for i in range(end_index):
            comment = self.comment_list[i]
            comment_str_date = comment.pubdate.strftime("%Y-%m-%d %H:%M:%S")
            if quote_uname == comment.user_name and quote_date == comment_str_date:
                return comment
                
        # not found, but should be found
        return None
        
    def sort_comment(self):
        """ 在完成对该topic的基本信息和所有comment的抽取后，对comment按照时间排序，
        如果某条comment引用之前的评论，则需要设置引用的comment id
        """
        # 对评论进行排序，按照发表时间从小到大
        self.comment_list = sorted(self.comment_list, key=operator.attrgetter('pubdate'), reverse = False)
        
        comment_count = len(self.comment_list)
        for i in range(comment_count):
            comment = self.comment_list[i]
            # 在评论内容中查找类似“@小C厨娘 24楼 2014-05-08 22:04:36”
            mlist = list(regex_quote.finditer(comment.content))
            if len(mlist) == 0: # 没有发现对应的模板
                continue
            # 查找引用
            m = mlist[-1]
            uname = m.group('uname')
            date = m.group('date')
            quote_comment = self.find_previous_comment(i, uname, date)
            if quote_comment is None:
                log.error('Quote comment not found for comment: %s in post: %s, in group: \%s' % (comment.cid, self.post_id, self.section_id))
                log.error('Current comment content: %s\n\n' % comment.content)
            else:
                # 链接找到的comment
                comment.quote = quote_comment
                #print comment.get_simple_string("[=]")
                log.info(u'评论 %s by %s 引用 评论 %s by %s' % (comment.cid, comment.user_name, comment.quote.cid, comment.quote.user_name))
        
if __name__ == "__main__":
    import sys
    import codecs
    sys.stdout = (codecs.getwriter('utf8'))(sys.stdout)
    
    congifLogger("log/models.log", 5)
    
    post = Post('4318716', u'free')
    #f = codecs.open(u"./testpage/舌尖上的厨娘 （配图，配过程），挑战你的味蕾_天涯杂谈_天涯论坛.html", "r", 'utf8') # first page
    f = codecs.open(u"./testpage/温故512：汶川地震的坍塌及重建_天涯杂谈_天涯论坛.html", "r", 'utf8') # first page    
    strfile_page1 = f.read()
    f.close()
    """
    f = codecs.open(u"./testpage/舌尖上的厨娘 （配图，配过程），挑战你的味蕾(第2页)_天涯杂谈_天涯论坛.html", "r", 'utf8') # first page    
    strfile_page2 = f.read()
    f.close()
    f = codecs.open(u"./testpage/舌尖上的厨娘 （配图，配过程），挑战你的味蕾(第3页)_天涯杂谈_天涯论坛.html", "r", 'utf8') # first page    
    strfile_page3 = f.read()
    f.close()
    """
    # 抓取评论
    comment_list1 = post.extract_first_page(strfile_page1)
    #comment_list2 = post.extract_nonfirst_page(strfile_page2)
    #comment_list3 = post.extract_nonfirst_page(strfile_page3)
    
    post.sort_comment()
    
    print post.__repr__()


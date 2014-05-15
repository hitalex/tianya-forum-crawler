#coding:utf8

# 这里存放所有用于匹配的正则表达式

import re

# 讨论帖链接模式
regex_post = re.compile(ur'(http://bbs\.tianya\.cn)?/post-(?P<section_id>[a-z]+)-(?P<post_id>\d+)-(?P<page_index>\d+)\.shtml')
regex_post_first = re.compile(ur'(http://bbs\.tianya\.cn)?/post-(?P<section_id>[a-z]+)-(?P<post_id>\d+)-1\.shtml')

# next page 模式
regex_next_page = re.compile(ur"http://bbs\.tianya\.cn/list\.jsp\?item=free&nextid=(\d+)")

# 用户链接模式
regex_user = re.compile(ur"^http://www\.tianya\.cn/(\d+)")

regex_quote = re.compile(ur'@(?P<uname>.*?)\s*(?P<lou_index>\d+)楼\s*(?P<date>\d+-\d+-\d+\s\d+:\d+:\d+)')

#coding:utf8

# 这里存放所有用于匹配的正则表达式

import re

# 讨论帖链接模式
regex_thread = re.compile(ur'^http://bbs\.tianya\.cn/post-free-(\d+)-1\.shtml')

# next page 模式
regex_next_page = re.compile(ur"^http://bbs\.tianya\.cn/list\.jsp\?item=free&nextid=(\d+)")

# 用户链接模式
regex_user = re.compile(ur"^http://www\.tianya\.cn/(\d+)")

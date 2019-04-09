# !/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'enzhao'
# Modified by enzhao on 2019/01/10.

# constant 
upper_chinese_number = ['一','二','三','四','五','六','七','八','九']
arabic_numerals_in_circle = ["①","②","③","④","⑤","⑥","⑦","⑧","⑨","⑩","⑪","⑫","⑬","⑭","⑮","⑯","⑰","⑱","⑲","⑳"]
chinese_dot = "、"
chinese_colon = "："

# Tools t
# show list elements for support utf-8
def show_list(_list = []) :
  for x in _list:
    print x

def filter_empty(_list = []):
  return filter(lambda x : len(x) > 0, _list)

def split_first(_str = "", _seperator = "", _is_warning = False):
  content_splits = filter_empty(_str.split(_seperator))
  if(len(content_splits) < 2 and _is_warning):
    print "WARNNING: return itself %s " % _str
    return _seperator.join([_str])
  return _seperator.join(content_splits[1:])

def str_is_int_digit(_str_value = ""):
  int_number_str = ["1","2","3","4","5","6","7","8","9"]
  return _str_value in int_number_str

def add_serial_number(_list = []):
  ids = range(0,len(_list))
  return [ str(idx + 1) + chinese_dot + _list[idx] for idx in ids ]


# convert elem src to target
# src_type
# key : 拓展阅读
# list :
# ① 超级话题物料全量上线
# ② 无限流向下刷新灰度3%，待效果评估
# ③ 微博协同过滤实验完成，待评估效果数据
# ④ 新标签体系实验待上线
# target_type
# 1、拓展阅读：超级话题物料全量上线
# 2、拓展阅读：无限流向下刷新灰度3%，待效果评估
# 3、拓展阅读：微博协同过滤实验完成，待评估效果数据
# 4、拓展阅读：新标签体系实验待上线
def convert_elem(_key = '', _list = []):
  if(len(_list) > 0):
    return [ _key + chinese_colon + split_first(_str = line, _seperator = ' ', _is_warning = True) for line in _list]
  else:
    return []
  

# convert elem-part src to target
# src_type
# 1、拓展阅读
# ① 超级话题物料全量上线
# ② 无限流向下刷新灰度3%，待效果评估
# ③ 微博协同过滤实验完成，待评估效果数据
# ④ 新标签体系实验待上线
# target_type
# 1、拓展阅读：超级话题物料全量上线
# 2、拓展阅读：无限流向下刷新灰度3%，待效果评估
# 3、拓展阅读：微博协同过滤实验完成，待评估效果数据
# 4、拓展阅读：新标签体系实验待上线
def convert_elem_part(_list = []) :
  assert(len(_list) > 0 and str_is_int_digit(_list[0][0]))
  key = split_first(_str = _list[0], _seperator = chinese_dot)
  key = key if(len(key) < len(_list[0]) and len(key) != 0) else split_first(_str = _list[0], _seperator = ' ')
  if(key[-1] == ":"):
    key = key[0:-1]
  if(key[-3:] == chinese_colon):
    key = key[0:-3]
  print "CONVERTING WITH KEY : %s" % key
  # show_list(_list)
  return convert_elem(_key = key, _list = _list[1:])

def find_elem_part_header(_list = []):
  idx_list = [ idx for idx,value in enumerate(_list) if(str_is_int_digit(value[0]))] 
  return idx_list if(len(idx_list) > 0) else [0]

# convert a part as follow:
# 一、正文页项目
# 1、拓展阅读
# ① 超级话题物料全量上线
# ② 无限流向下刷新灰度3%，待效果评估
# to target type as follow:
# 一、正文页项目
# 1、拓展阅读：超级话题物料全量上线
# 2、拓展阅读：无限流向下刷新灰度3%，待效果评估
def convert_part(_list = []):
  assert(len(_list) > 0)
  title = _list[0]
  if(title[0:3] not in upper_chinese_number):
    print "contents of part contain elem_parts has a wrong title!!"
    print "WRONG TITLE : " + title
    return []
  print "CONVERTING WITH %s" % title
  contents = _list[1:]
  elem_part_header_idx = find_elem_part_header(contents) + [None]
  elem_parts = [ 
  contents[elem_part_header_idx[i-1] : elem_part_header_idx[i]] 
  for i in range(1,len(elem_part_header_idx))
  ]
  temp_header = "1" + chinese_dot + split_first(title, chinese_dot)
  # elem_parts = elem_parts if(len(elem_part_header_idx) > 2) else [temp_header] + elem_parts
  new_content_parts = reduce(lambda x,y : x + y , [
  convert_elem_part(elem_part) for elem_part in elem_parts
  ])
  # print
  new_content_parts_with_serial_number = add_serial_number(new_content_parts)
  return [title] + new_content_parts_with_serial_number

def find_part_header(_list = []):
  return [ idx for idx,value in enumerate(_list) if(value[0:3] in upper_chinese_number)]

# convert a big part as follow:
# >>  业务
# 一、正文页项目
# 1、拓展阅读
# ① 超级话题物料全量上线
# ② 无限流向下刷新灰度3%，待效果评估
# to target type as follow:

def convert_big_part(_list = []):
  assert(len(_list) > 0)
  title = _list[0]
  if(title[0:2] != ">>"):
    print "contents of big part contain elem_parts has a wrong title!!"
    print "WRONG TITLE : " + title
    return []
  contents = _list[1:]
  part_header_idx = find_part_header(contents) + [None]
  # print part_header_idx
  parts = [ 
  contents[part_header_idx[i-1] : part_header_idx[i]] 
  for i in range(1,len(part_header_idx))
  ]
  new_content_parts = reduce(lambda x,y : x + y , [
  convert_part(part) for part in parts
  ])
  return [title] + new_content_parts

import os,sys
src_date = sys.argv[1] if(len(sys.argv) > 1) else None
src_date = 20190110
if(src_date == None):
  print u'usage : src_file need to be 周报-feed算法-黄波-${date}.txt'
# print sys.argv

# read src_file and trim them
src_file_name_base = u"周报-feed算法-黄波-%s.txt" % src_date
target_file_name_base = u"机器学习研发-机器学习架构周报-%s.txt" % src_date
with open(src_file_name_base,"r") as file_handler:
  src_content = file_handler.readlines()
src_data = [line.strip() for line in src_content]
(field_dilimeter_1, field_dilimeter_2) = [ line for line in src_data if line.startswith(">>") ]

# data part split
# 二、业务应用进展
# >>  业务
header = src_data[:src_data.index(field_dilimeter_1)]
# >>  业务 ~ >> 机器学习平台建设进展
bussiness_content = src_data[src_data.index(field_dilimeter_1): src_data.index(field_dilimeter_2)]
# >> 机器学习平台建设进展 to end
platform_content = src_data[src_data.index(field_dilimeter_2): ]

# data convert
new_bussiness_content = convert_big_part(bussiness_content)
new_platform_content = convert_big_part(platform_content)
# show_list(header)
# show_list(new_bussiness_content)
# show_list(new_platform_content)
new_content = header + new_bussiness_content + new_platform_content
with open(target_file_name_base, "w") as file_writer:
  for line in new_content:
    file_writer.write(line + "\n")
  file_writer.flush()

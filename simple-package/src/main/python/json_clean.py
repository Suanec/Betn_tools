#! /usr/bin/python
# -*- coding: utf-8 -*-

import json
dirty = map(lambda line: json.loads(line),open("./sample.json.dirty","r").readlines())
for line in dirty:
    line.pop("label")
    for k in line.keys():
        if("WEIBO_DIRTY_DATA" in str(line[k])):
            line.pop(k)
json_dirty_cleaned = map(lambda line: json.dumps(line),dirty)
with open("./sample.json.clean","w") as json_writer:
     map(lambda line: json_writer.write(line + "\n"), json_dirty_cleaned)


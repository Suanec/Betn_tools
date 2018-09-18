import json
from collections import Iterable
def json2dict(_json_str = ""):
    return json.loads(_json_str)

def dict2json(_dict = {}):
    return json.dumps(_dict)

def mk_str(_obj = Iterable, _delimiter = ",", _indentation = ""):
    items_value = ""
    if len(_obj) < 1 : return items_value
    for item in _obj[:-1] :
        if isinstance(item,dict):
            items_value += dict2xml(item, _indentation)
        else:
            items_value += str(item) + _delimiter
    if isinstance(_obj[-1],dict):
        items_value += dict2xml(_obj[-1], _indentation)
    else:
        items_value += str(_obj[-1]) + "\n"
    return items_value

def dict2xml(_json_dict = {}, _indentation = ""):
    rst_str = ""
    for key in _json_dict.keys():
        value = _json_dict.get(key)
        if hasattr(value,"__iter__"):
            if isinstance(value,dict):
                value_str = dict2xml(value, _indentation + "    ")
                rst_str += """{0}<{1}>\n{0}{2}</{1}>\n""".format(_indentation, key, value_str)
            else: 
                xml_value = mk_str(value,_indentation = "    " + _indentation)
                rst_str += "{0}<{1}>\n{2}{0}</{1}>\n".format(_indentation, key, xml_value)
        else:
            rst_str += "{0}<{1}>{2}</{1}>\n".format(_indentation, key, value)
    return rst_str


lint = [1,2,3]
lstr = ["1","2",'3']
req_dict = json.loads('{"message":"success","result":[{"timestamp":"1511858444","id":"1","other":"{}","importance":"0","url":"","size":"1024","version":"1.0.0"}],"params":{},"code":200}')
dd = {"req":req_dict,"lint":lint,"lstr":lstr,"int":3,"str":"asddiid"}
print dict2xml(dd)

